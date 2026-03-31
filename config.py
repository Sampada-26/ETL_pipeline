from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
SQLITE_DB_PATH = PROJECT_ROOT / "warehouse.db"

# Real API source (public GitHub repositories list)
API_URL = "https://api.github.com/search/repositories?q=stars:%3E10000&sort=stars&order=desc&per_page=20"
REQUEST_TIMEOUT_SECONDS = 15

# SQLite table names
RAW_TABLE = "raw_table"
STAGING_TABLE = "staging_table"
AUDIT_TABLE = "etl_job_audit"

# Defaults used during transformation
DEFAULT_REPO_NAME = "unknown_repo"
DEFAULT_FULL_NAME = "unknown/unknown_repo"
DEFAULT_OWNER_LOGIN = "unknown_owner"
DEFAULT_OWNER_TYPE = "unknown_type"
DEFAULT_LANGUAGE = "unknown_language"
DEFAULT_CREATED_AT = "unknown_created_at"
DEFAULT_STARS = 0
DEFAULT_FORKS = 0
DEFAULT_OPEN_ISSUES = 0
DEFAULT_WATCHERS = 0
DEFAULT_PRIVATE = False
