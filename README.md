# ETL Pipeline Project (Python + SQLite)

This project demonstrates a simple ETL pipeline using:
- `Python`
- `SQLite` as a mini data warehouse
- `Pandas` for transformations
- A popular public API: `https://dummyjson.com/users`

It also includes fallback local data if internet is not available.

## Project requirements covered

1. Using SQLite in Python  
`sqlite3` + `pandas.to_sql()` are used to load tables.

2. Datatype change  
`age`, `height_mm`, `weight_kg` are converted to numeric.

3. mm to cm  
`height_mm` is transformed to `height_cm`.

4. Write Python script  
All logic is in [etl_pipeline.py](/home/sampada-kaginkar/Videos/etl/ETL_pipeline/etl_pipeline.py).

5. Add missing age values  
Missing `age` values are filled with median age.

6. Manipulation on data  
Adds `bmi`, `age_group`, `email_domain`, and city cleaning.

7. API transformed data into warehouse  
Data is extracted from API, transformed, and loaded into warehouse tables.

8. Simulate Amazon ETL tool flow  
Script simulates:
- S3 raw landing
- AWS Glue transform
- Redshift warehouse
- CloudWatch logs (audit table)

9. Perform ETL pipeline using script  
Run one command and the full pipeline executes.

## Files

- [config.py](/home/sampada-kaginkar/Videos/etl/ETL_pipeline/config.py): constants and settings
- [etl_pipeline.py](/home/sampada-kaginkar/Videos/etl/ETL_pipeline/etl_pipeline.py): ETL code
- `warehouse.db`: generated SQLite data warehouse

## How to run

1. Activate environment (if needed):
```bash
source venv/bin/activate
```

2. Run ETL:
```bash
./venv/bin/python etl_pipeline.py
```

3. Confirm output tables in SQLite:
```bash
./venv/bin/python - <<'PY'
import sqlite3
from config import SQLITE_DB_PATH
conn = sqlite3.connect(SQLITE_DB_PATH)
for t in ["raw_users", "stg_users_cleaned", "dw_user_profile", "dw_city_metrics", "etl_job_audit"]:
    c = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"{t}: {c}")
conn.close()
PY
```

## Step-by-step ETL flow (simple)

1. Extract  
Fetch user data from API.

2. Transform  
- Convert datatypes
- Fill missing ages
- Convert mm to cm
- Create new analysis columns

3. Load  
Save into SQLite tables:
- `raw_users` (raw zone)
- `stg_users_cleaned` (cleaned zone)
- `dw_user_profile` (warehouse detail)
- `dw_city_metrics` (warehouse aggregate)
- `etl_job_audit` (run logs)

## Useful SQL queries (for demo)

```sql
SELECT * FROM dw_user_profile LIMIT 5;
SELECT city, users_count, avg_age FROM dw_city_metrics ORDER BY users_count DESC;
SELECT * FROM etl_job_audit ORDER BY rowid DESC LIMIT 5;
```
