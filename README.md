# ETL Pipeline (Python + SQLite)

A beginner-friendly ETL project that extracts GitHub repository data, transforms it with Pandas, and loads it into SQLite.

## Project Overview

This pipeline follows a simple ETL flow:

1. Extract repository data from the GitHub Search API
2. Show the raw data in the terminal
3. Transform and clean the dataset
4. Load both raw and transformed data into SQLite
5. Record each run in an audit table

## ETL Flow

```text
GitHub API -> RAW DATA -> Transform -> TRANSFORMED DATA -> SQLite
```

## What Gets Transformed

The script in `etl_pipeline.py` performs these transformations:

- fills missing values for repo, owner, language, and numeric metrics
- converts numeric fields like stars, forks, issues, and watchers
- normalizes text fields such as `owner_lower` and `language_upper`
- creates derived columns like `popularity` and `activity`
- removes duplicate rows based on repository id

## SQLite Tables

| Table | Purpose |
|---|---|
| `raw_table` | Stores extracted GitHub repository data |
| `staging_table` | Stores cleaned and transformed data |
| `etl_job_audit` | Stores ETL run metadata such as run ID, row counts, and status |

## Project Files

```text
ETL_pipeline/
|-- config.py
|-- etl_pipeline.py
|-- requirements.txt
|-- README.md
|-- warehouse.db
`-- venv/
```

## Requirements

- Python 3
- Pandas
- Requests
- Internet access for the GitHub API call

## How To Run

Open a terminal in your project folder first. For example:

```bash
cd /path/to/ETL_pipeline
```

### Option 1: Run with the existing virtual environment

```bash
./venv/bin/python etl_pipeline.py
```

### Option 2: Create a fresh virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python etl_pipeline.py
```

## Expected Output

When the pipeline runs successfully, you will see stages like:

- `[EXTRACT] Fetching repositories from GitHub API...`
- `RAW GITHUB DATA`
- `[TRANSFORM] Cleaning and transforming data...`
- `TRANSFORMED GITHUB DATA`
- `[LOAD] Loading data into SQLite...`
- `ETL SUMMARY`
- `Pipeline completed successfully.`

## Verify The Data

You can inspect the SQLite tables with:

```bash
sqlite3 warehouse.db
```

Then run:

```sql
SELECT COUNT(*) FROM raw_table;
SELECT COUNT(*) FROM staging_table;
SELECT * FROM etl_job_audit ORDER BY rowid DESC LIMIT 5;
```

## Configuration

You can change the pipeline settings in `config.py`, including:

- API URL
- request timeout
- SQLite database path
- table names
- default values used during transformation
