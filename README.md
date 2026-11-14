# CMS Hospitals download job

Downloads every dataset tagged with the "Hospitals" theme from the CMS Provider Data metastore, converts the CSV column names to snake_case, and saves the files locally.

Source: https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

## Running it

Needs Python 3.9+. Works on Windows or Linux.

```
pip install -r requirements.txt
python cms_hospitals_job.py
```

Other options:

```
python cms_hospitals_job.py --dry-run       # list what would download
python cms_hospitals_job.py --full-refresh  # ignore saved state, download everything
python cms_hospitals_job.py --workers 8     # number of parallel downloads (default 6)
python cms_hospitals_job.py --self-test     # check the snake_case conversion
```

Output goes to `data/hospitals/`. Run history and logs go to `state/`.

## How it works

- **Finding the datasets.** It reads the metastore and keeps every dataset whose `theme` includes "Hospitals" (75 of 237 when I tested it).
- **Parallel downloads.** The CSVs are downloaded and processed at the same time with a thread pool. Each file is streamed to disk and copied row by row, so memory stays low. Only the header row changes; the data is untouched.
- **Only downloading what changed.** A small SQLite file (`state/job_metadata.db`) stores each run and, for every dataset, the `modified` date from the last time it was processed. A dataset is downloaded only if it's new, its `modified` date has changed, or its output file is missing.

  I compare each dataset's stored `modified` date rather than "changed since the last run time", because `modified` is only a date with no time. This way a skipped or failed day doesn't cause anything to be missed. A dataset is only marked done after its file is fully written, so a failed download is retried the next day.
- **Safety.** Failed requests retry with backoff, and one bad file doesn't stop the others. Each file is written to a temp name and then renamed, so a half-written CSV never appears. A lock file stops two runs overlapping. Each run writes a JSON manifest under `state/manifests/` showing what changed and the header mapping.

## snake_case rules

- Apostrophes are removed: `Patients’ rating of the facility linear mean score` → `patients_rating_of_the_facility_linear_mean_score`
- Spaces and symbols become single underscores: `City/Town` → `city_town`
- camelCase is split (`measureID` → `measure_id`) and accents become plain letters
- A name starting with a number gets `col_` in front
- If two columns end up with the same name, the later ones get `_2`, `_3`

## Running it daily

Linux (cron, 6:15 every morning):

```
15 6 * * * cd /opt/cms_hospitals && .venv/bin/python cms_hospitals_job.py
```

Windows (Task Scheduler):

```
schtasks /Create /SC DAILY /ST 06:15 /TN "CMS Hospitals Job" /TR "C:\cms\.venv\Scripts\python.exe C:\cms\cms_hospitals_job.py"
```

Exit code is 0 if everything worked, 1 if some datasets failed (they're retried next run), and 2 if the run itself failed.

## Sample output

In `sample_output/`:

- `sample_output.txt` contains the self-test results, the first-run log, all 75 Hospitals datasets with their output file names, and two back-to-back runs on Hospital General Information. The first run processes the file (5,419 rows, 38 columns) and the second skips it because nothing changed. It also has the full original → snake_case header mapping.
- `hospital_general_information__xubh-q36u.csv` is that file after processing.
