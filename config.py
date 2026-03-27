from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
SQLITE_DB_PATH = PROJECT_ROOT / "warehouse.db"

# Single API source (products)
API_URL = "https://dummyjson.com/products?limit=20"
REQUEST_TIMEOUT_SECONDS = 15

# SQLite table names
RAW_TABLE = "raw_table"
STAGING_TABLE = "staging_table"
AUDIT_TABLE = "etl_job_audit"

# Defaults used during transformation
DEFAULT_PRICE = 0.0
DEFAULT_RATING = 0.0
DEFAULT_STOCK = 0
DEFAULT_DISCOUNT_PERCENTAGE = 0.0
