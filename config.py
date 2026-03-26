from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
SQLITE_DB_PATH = PROJECT_ROOT / "warehouse.db"

# Popular public API for demo data
API_URL = "https://dummyjson.com/users?limit=30"
REQUEST_TIMEOUT_SECONDS = 15

# SQLite table names
RAW_TABLE = "raw_users"
STAGING_TABLE = "stg_users_cleaned"
WAREHOUSE_DETAIL_TABLE = "dw_user_profile"
WAREHOUSE_CITY_TABLE = "dw_city_metrics"
AUDIT_TABLE = "etl_job_audit"

# Transformation settings
SIMULATE_MISSING_AGE_EVERY_N = 6
DEFAULT_AGE_FILL = 30
