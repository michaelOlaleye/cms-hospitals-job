#!/usr/bin/env python3
"""
Pulls every "Hospitals" dataset from the CMS Provider Data metastore,
converts the CSV headers to snake_case and saves them locally.

Meant to run once a day. Each run checks the metastore 'modified' date for
every dataset against what was saved last time, and only downloads the ones
that are new or changed. Run history and per-dataset state are kept in a
small SQLite file.

Setup:
    pip install -r requirements.txt

Run:
    python cms_hospitals_job.py                 # normal daily run
    python cms_hospitals_job.py --dry-run       # just list what would download
    python cms_hospitals_job.py --full-refresh  # ignore saved state
    python cms_hospitals_job.py --self-test     # check the snake_case function

Output:
    data/hospitals/<title>__<id>.csv    processed files
    state/job_metadata.db               run history + dataset state
    state/manifests/run_NNNNN.json      what each run did (incl. header mapping)
    state/logs/YYYY-MM-DD.log

Scheduling:
    cron:   15 6 * * * cd /opt/cms_hospitals && .venv/bin/python cms_hospitals_job.py
    Windows Task Scheduler:
      schtasks /Create /SC DAILY /ST 06:15 /TN "CMS Hospitals Job"
               /TR "C:\\cms\\.venv\\Scripts\\python.exe C:\\cms\\cms_hospitals_job.py"

Exit code is 0 if everything worked, 1 if some datasets failed (they get
retried next run), 2 if the run itself failed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
THEME = "Hospitals"
WORKERS = 6
TIMEOUT = (15, 300)  # connect, read

log = logging.getLogger("cms_hospitals")

# some CMS footnote fields are long
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


# ---------------------------------------------------------------- snake_case

def to_snake_case(name):
    # drop accents, e.g. é -> e
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    # remove apostrophes instead of splitting on them: Patients’ -> patients
    s = re.sub(r"['\u2018\u2019`]", "", s)
    # split camelCase: measureID -> measure_ID
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    # anything else that isn't a letter/number becomes one underscore
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()
    if not s:
        return "unnamed"
    if s[0].isdigit():
        s = "col_" + s  # keep it a valid identifier for SQL/pandas
    return s


def snake_case_headers(headers):
    # if two headers end up the same, add _2, _3 so nothing gets overwritten
    out, seen = [], {}
    for h in headers:
        name = to_snake_case(h)
        base = name
        while name in seen:
            seen[base] += 1
            name = f"{base}_{seen[base]}"
        seen[name] = 1
        out.append(name)
    return out


# ---------------------------------------------------------------- state db

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT, finished_at TEXT,
    status TEXT,                      -- running / success / partial / failed
    datasets_seen INTEGER DEFAULT 0, datasets_new INTEGER DEFAULT 0,
    datasets_skipped INTEGER DEFAULT 0, datasets_failed INTEGER DEFAULT 0,
    notes TEXT
);
CREATE TABLE IF NOT EXISTS datasets (
    identifier TEXT PRIMARY KEY,
    title TEXT, modified TEXT, download_url TEXT, output_path TEXT,
    row_count INTEGER, column_count INTEGER, sha256 TEXT,
    last_run_id INTEGER, last_success_at TEXT
);
CREATE TABLE IF NOT EXISTS dataset_runs (
    run_id INTEGER, identifier TEXT,
    status TEXT,                      -- downloaded / skipped / failed
    modified TEXT, seconds REAL, error TEXT,
    PRIMARY KEY (run_id, identifier)
);
"""


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class State:
    # only used from the main thread; the download threads never touch the db

    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    def start_run(self):
        cur = self.db.execute("INSERT INTO runs (started_at, status) VALUES (?, 'running')", (now(),))
        self.db.commit()
        return cur.lastrowid

    def finish_run(self, run_id, status, seen, new, skipped, failed, notes=""):
        self.db.execute(
            "UPDATE runs SET finished_at=?, status=?, datasets_seen=?, datasets_new=?, "
            "datasets_skipped=?, datasets_failed=?, notes=? WHERE run_id=?",
            (now(), status, seen, new, skipped, failed, notes, run_id))
        self.db.commit()

    def last_good_run(self):
        return self.db.execute(
            "SELECT * FROM runs WHERE status IN ('success','partial') "
            "ORDER BY run_id DESC LIMIT 1").fetchone()

    def get(self, identifier):
        return self.db.execute("SELECT * FROM datasets WHERE identifier=?", (identifier,)).fetchone()

    def save_dataset(self, run_id, r):
        self.db.execute(
            "INSERT OR REPLACE INTO datasets VALUES (?,?,?,?,?,?,?,?,?,?)",
            (r.identifier, r.title, r.modified, r.download_url, r.output_path,
             r.row_count, r.column_count, r.sha256, run_id, now()))
        self.log_dataset(run_id, r.identifier, "downloaded", r.modified, r.seconds)

    def log_dataset(self, run_id, identifier, status, modified, seconds=None, error=None):
        self.db.execute("INSERT OR REPLACE INTO dataset_runs VALUES (?,?,?,?,?,?)",
                        (run_id, identifier, status, modified, seconds, error))
        self.db.commit()


# ---------------------------------------------------------------- metastore

@dataclass
class Dataset:
    identifier: str
    title: str
    modified: str
    download_url: str


@dataclass
class Result:
    identifier: str
    title: str
    modified: str
    download_url: str
    output_path: str
    row_count: int
    column_count: int
    sha256: str
    seconds: float
    original_headers: list
    new_headers: list


def http_session():
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods={"GET"}, respect_retry_after_header=True)
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=32))
    s.mount("http://", HTTPAdapter(max_retries=retry, pool_maxsize=32))
    return s


def get_hospital_datasets(session, url, theme):
    log.info("Fetching metastore: %s", url)
    resp = session.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    items = resp.json()

    found = []
    for item in items:
        if theme.lower() not in [t.strip().lower() for t in item.get("theme", [])]:
            continue
        csv_url = next((d.get("downloadURL") for d in item.get("distribution", [])
                        if d.get("mediaType") == "text/csv"
                        or (d.get("downloadURL") or "").lower().endswith(".csv")), None)
        if not csv_url:
            log.warning("No CSV for %s (%s), skipping", item.get("identifier"), item.get("title"))
            continue
        found.append(Dataset(item["identifier"], item.get("title", item["identifier"]),
                             str(item.get("modified", "")), csv_url))

    log.info("Metastore: %d items total, %d with theme '%s'", len(items), len(found), theme)
    return found


def why_download(ds, saved, full_refresh):
    """Returns a reason string if the dataset needs downloading, else None.

    I compare against the 'modified' value saved for this dataset rather than
    the time of the last run. 'modified' is just a date (no time/timezone),
    so this can't miss a same-day update and still works if a day is skipped.
    """
    if full_refresh:
        return "full refresh"
    if saved is None:
        return "new dataset"
    if saved["modified"] != ds.modified:
        return f"modified {saved['modified']} -> {ds.modified}"
    if not saved["output_path"] or not Path(saved["output_path"]).exists():
        return "output file missing"
    return None


# ---------------------------------------------------------------- download

def file_name(title, max_len=100):
    s = to_snake_case(title)
    if len(s) > max_len:
        s = s[:max_len].rsplit("_", 1)[0]  # cut on a word, not mid-word
    return s or "dataset"


def open_csv(path):
    # CMS files are UTF-8 (sometimes with a BOM); fall back to cp1252 just in case
    try:
        with open(path, encoding="utf-8-sig") as f:
            while f.read(1 << 20):
                pass
        return open(path, encoding="utf-8-sig", newline="")
    except UnicodeDecodeError:
        log.warning("%s isn't valid UTF-8, reading as cp1252", path.name)
        return open(path, encoding="cp1252", newline="")


def process(ds, out_dir, session):
    """Download one CSV and write it back out with snake_case headers.
    Runs in a worker thread."""
    start = time.monotonic()
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / f"{file_name(ds.title)}__{ds.identifier}.csv"
    fd, raw = tempfile.mkstemp(suffix=".raw", dir=out_dir)
    os.close(fd)
    raw, part = Path(raw), final.with_suffix(".csv.part")

    try:
        # stream to disk so big files don't sit in memory
        with session.get(ds.download_url, stream=True, timeout=TIMEOUT) as r:
            r.raise_for_status()
            with open(raw, "wb") as f:
                for chunk in r.iter_content(256 * 1024):
                    f.write(chunk)

        # rewrite the header, copy the rows as they are
        rows = 0
        with open_csv(raw) as fin, open(part, "w", encoding="utf-8", newline="") as fout:
            reader = csv.reader(fin)
            headers = next(reader, None)
            if headers is None:
                raise ValueError("file is empty")
            new_headers = snake_case_headers(headers)
            writer = csv.writer(fout, lineterminator="\n")
            writer.writerow(new_headers)
            for row in reader:
                writer.writerow(row)
                rows += 1

        sha = hashlib.sha256()
        with open(part, "rb") as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                sha.update(block)

        # swap in the finished file so nobody reads a half-written one
        os.replace(part, final)
    finally:
        raw.unlink(missing_ok=True)
        part.unlink(missing_ok=True)

    return Result(ds.identifier, ds.title, ds.modified, ds.download_url, str(final.resolve()),
                  rows, len(new_headers), sha.hexdigest(), round(time.monotonic() - start, 2),
                  headers, new_headers)


# ---------------------------------------------------------------- lock

class Lock:
    # stops a second run starting while the previous one is still going

    def __init__(self, path, stale_hours=6):
        self.path, self.stale = path, stale_hours * 3600

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists() and time.time() - self.path.stat().st_mtime > self.stale:
            log.warning("Removing old lock file %s", self.path)
            self.path.unlink()
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise SystemExit(f"Another run is already going (lock file {self.path})")
        with os.fdopen(fd, "w") as f:
            f.write(f"{os.getpid()} {now()}\n")
        return self

    def __exit__(self, *_):
        self.path.unlink(missing_ok=True)


# ---------------------------------------------------------------- main job

def run(args):
    out_dir, state_dir = Path(args.output_dir), Path(args.state_dir)
    state = State(state_dir / "job_metadata.db")
    session = http_session()

    prev = state.last_good_run()
    log.info("Previous successful run: %s",
             f"#{prev['run_id']} finished {prev['finished_at']}" if prev else "none (first run)")
    run_id = state.start_run()
    log.info("Starting run #%d", run_id)

    done, skipped, failed = [], [], {}
    try:
        datasets = get_hospital_datasets(session, args.metastore_url, args.theme)

        todo = []
        for ds in datasets:
            reason = why_download(ds, state.get(ds.identifier), args.full_refresh)
            if reason:
                log.info("  queue  %-10s %s  (%s)", ds.identifier, ds.title, reason)
                todo.append(ds)
            else:
                skipped.append(ds.identifier)
                state.log_dataset(run_id, ds.identifier, "skipped", ds.modified)
        log.info("%d to download, %d unchanged", len(todo), len(skipped))

        if args.dry_run:
            state.finish_run(run_id, "success", len(datasets), 0, len(skipped), 0, "dry run")
            return 0

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            jobs = {pool.submit(process, ds, out_dir, session): ds for ds in todo}
            for job in as_completed(jobs):
                ds = jobs[job]
                try:
                    res = job.result()
                except Exception as e:
                    # log it and keep going; it isn't saved as done, so it retries tomorrow
                    failed[ds.identifier] = f"{type(e).__name__}: {e}"
                    log.error("  FAIL   %-10s %s: %s", ds.identifier, ds.title, e)
                    state.log_dataset(run_id, ds.identifier, "failed", ds.modified,
                                      error=failed[ds.identifier])
                    continue
                state.save_dataset(run_id, res)
                done.append(res)
                log.info("  done   %-10s %6d rows %3d cols %6.1fs  %s", res.identifier,
                         res.row_count, res.column_count, res.seconds, Path(res.output_path).name)

        if not failed:
            status = "success"
        else:
            status = "partial" if done or skipped else "failed"
        state.finish_run(run_id, status, len(datasets), len(done), len(skipped), len(failed))
    except Exception as e:
        log.exception("Run #%d failed", run_id)
        state.finish_run(run_id, "failed", 0, len(done), len(skipped), len(failed), str(e))
        return 2

    manifest = {
        "run_id": run_id,
        "finished_at": now(),
        "status": status,
        "downloaded": [asdict(r) for r in sorted(done, key=lambda r: r.identifier)],
        "skipped_unchanged": sorted(skipped),
        "failed": failed,
    }
    path = state_dir / "manifests" / f"run_{run_id:05d}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info("Run #%d %s: %d downloaded, %d unchanged, %d failed. Manifest: %s",
             run_id, status.upper(), len(done), len(skipped), len(failed), path)
    return 0 if status == "success" else 1


# ---------------------------------------------------------------- self test

TEST_CASES = {
    "Patients’ rating of the facility linear mean score":
        "patients_rating_of_the_facility_linear_mean_score",
    "Facility ID": "facility_id",
    "City/Town": "city_town",
    "ZIP Code": "zip_code",
    "Hospital overall rating footnote": "hospital_overall_rating_footnote",
    "Number of Discharges (%)": "number_of_discharges",
    "  Score - Lower/Upper  ": "score_lower_upper",
    "Hospital's Name": "hospitals_name",
    "MSPB-1 Score": "mspb_1_score",
    "measureID": "measure_id",
    "Café Né": "cafe_ne",
    "30-day mortality": "col_30_day_mortality",
    "": "unnamed",
}


def self_test():
    bad = 0
    for raw, expected in TEST_CASES.items():
        got = to_snake_case(raw)
        bad += got != expected
        print(f"{'PASS' if got == expected else 'FAIL'}  {raw!r:58} -> {got}")
    dup = snake_case_headers(["Score", "score", "SCORE "])
    bad += dup != ["score", "score_2", "score_3"]
    print(f"{'PASS' if dup == ['score', 'score_2', 'score_3'] else 'FAIL'}  "
          f"duplicate headers ['Score','score','SCORE '] -> {dup}")
    print(f"\n{len(TEST_CASES) + 1 - bad} passed, {bad} failed")
    return 1 if bad else 0


def main():
    p = argparse.ArgumentParser(description="Download CMS 'Hospitals' datasets (incremental).")
    p.add_argument("--output-dir", default="data/hospitals")
    p.add_argument("--state-dir", default="state")
    p.add_argument("--workers", type=int, default=WORKERS)
    p.add_argument("--theme", default=THEME)
    p.add_argument("--metastore-url", default=METASTORE_URL)
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args()

    if args.self_test:
        return self_test()

    state_dir = Path(args.state_dir)
    (state_dir / "logs").mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
    handlers = [logging.StreamHandler(sys.stdout),
                logging.FileHandler(state_dir / "logs" / f"{datetime.now():%Y-%m-%d}.log",
                                    encoding="utf-8")]
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    with Lock(state_dir / "job.lock"):
        return run(args)


if __name__ == "__main__":
    sys.exit(main())
