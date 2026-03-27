# Simple ETL Pipeline (Products API + SQLite)

This project has a simple ETL pipeline in Python.

## What it does

1. Extracts product data from one API: `https://dummyjson.com/products?limit=20`
2. If API fails, it uses fallback local product data
3. Prints **RAW DATA** (before transformation)
4. Transforms data:
   - fills missing values
   - converts datatypes
   - converts discount percentage to ratio
   - creates final price after discount
   - normalizes text fields
5. Prints **TRANSFORMED DATA** (after transformation)
6. Loads both datasets to SQLite

## SQLite tables

- `raw_table` -> raw extracted data
- `staging_table` -> transformed data
- `etl_job_audit` -> pipeline run logs

## Run

```bash
pip install -r requirements.txt
./venv/bin/python etl_pipeline.py
```

## Files

- `config.py` -> settings/constants
- `etl_pipeline.py` -> ETL code
- `warehouse.db` -> SQLite database (created/updated after run)
