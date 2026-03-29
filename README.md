# ETL Pipeline (Python + SQLite)

A clean and beginner-friendly ETL project that extracts product data, transforms it, and loads it into SQLite.

## Project Overview

This project simulates a real ETL workflow:

1. **Extract** product data from an API (`dummyjson`)  
2. Use **fallback local data** if API is unavailable  
3. Display **RAW DATA** clearly before transformation  
4. **Transform** data by cleaning, type conversion, and derived calculations  
5. Display **TRANSFORMED DATA** clearly after transformation  
6. **Load** raw and transformed data into SQLite tables with an audit log  

## ETL Flow

```text
API / Fallback -> RAW DATA -> Transform -> TRANSFORMED DATA -> SQLite
```

## Key Transformations

| Transformation | Description |
|---|---|
| Missing value handling | Fills null values for price, rating, stock, etc. |
| Type conversion | Converts numeric fields to proper numeric data types |
| Discount conversion | Converts `discountPercentage` to `discount_ratio` |
| Derived metric | Creates `final_price` after discount |
| Text normalization | Cleans title/category/brand fields |

## SQLite Tables

| Table | Purpose |
|---|---|
| `raw_table` | Stores extracted raw product data |
| `staging_table` | Stores cleaned and transformed product data |
| `etl_job_audit` | Stores ETL run metadata (run id, row counts, status) |

## File Structure

```text
ETL_pipeline/
|-- config.py
|-- etl_pipeline.py
|-- requirements.txt
|-- README.md
|-- warehouse.db
|-- __pycache__/
`-- venv/
```

## Tech Stack

- Python 3
- Pandas
- Requests
- SQLite (`sqlite3`)

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run the pipeline:

```bash
./venv/bin/python etl_pipeline.py
```

## Output Stages You Will See

- `[EXTRACT]` API fetch / fallback message  
- `RAW DATA (Before Transformation)` section  
- `[TRANSFORM]` transformation stage message  
- `TRANSFORMED DATA (After Transformation)` section  
- `[LOAD]` SQLite load message  
- `ETL SUMMARY` with row counts and table names  

## Sample SQL Checks

```sql
SELECT COUNT(*) FROM raw_table;
SELECT COUNT(*) FROM staging_table;
SELECT * FROM etl_job_audit ORDER BY rowid DESC LIMIT 5;
```

## Configuration

All tunable values are in `config.py`:

- API URL
- request timeout
- table names
- default values used in transformations
