import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

try:
    import pandas as pd
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency: pandas. Install with `pip install -r requirements.txt` "
        "or run with `./venv/bin/python etl_pipeline.py`."
    ) from exc

try:
    import requests
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency: requests. Install with `pip install -r requirements.txt` "
        "or run with `./venv/bin/python etl_pipeline.py`."
    ) from exc

from config import (
    API_URL,
    AUDIT_TABLE,
    DEFAULT_CREATED_AT,
    DEFAULT_FORKS,
    DEFAULT_LANGUAGE,
    DEFAULT_OPEN_ISSUES,
    DEFAULT_OWNER_LOGIN,
    DEFAULT_OWNER_TYPE,
    DEFAULT_PRIVATE,
    DEFAULT_REPO_NAME,
    DEFAULT_STARS,
    DEFAULT_WATCHERS,
    RAW_TABLE,
    REQUEST_TIMEOUT_SECONDS,
    SQLITE_DB_PATH,
    STAGING_TABLE,
)

RAW_COLUMNS = [
    "id",
    "repo",
    "owner",
    "owner_type",
    "language",
    "stars",
    "forks",
    "issues",
    "watchers",
    "private",
    "created_on",
]

TRANSFORMED_PREVIEW_COLUMNS = [
    "id",
    "repo",
    "owner",
    "language_upper",
    "stars",
    "forks",
    "issues",
    "watchers",
    "popularity",
    "activity",
]

STAGING_COLUMNS = [
    "id",
    "repo",
    "owner",
    "owner_lower",
    "owner_type",
    "language",
    "language_upper",
    "stars",
    "forks",
    "issues",
    "watchers",
    "popularity",
    "activity",
    "private",
    "created_on",
    "cleaned_on",
]


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def extract_data():
    print("[EXTRACT] Fetching repositories from GitHub API...")

    try:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "beginner-etl-pipeline",
        }
        response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS, headers=headers)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        raise SystemExit(f"[EXTRACT] API fetch failed: {exc}")

    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = payload.get("items", [])
    else:
        records = []

    if not records:
        raise SystemExit("[EXTRACT] API returned no repository rows. Update API_URL in config.py")

    rows = []
    for repo in records:
        if not isinstance(repo, dict):
            continue

        owner = repo.get("owner") or {}
        row = {
            "id": repo.get("id"),
            "repo": repo.get("name"),
            "owner": owner.get("login"),
            "owner_type": owner.get("type"),
            "language": repo.get("language"),
            "stars": repo.get("stargazers_count"),
            "forks": repo.get("forks_count"),
            "issues": repo.get("open_issues_count"),
            "watchers": repo.get("watchers_count"),
            "private": repo.get("private"),
            "created_on": repo.get("created_at"),
        }
        rows.append(row)

    raw_df = pd.DataFrame(rows)

    for column_name in RAW_COLUMNS:
        if column_name not in raw_df.columns:
            raw_df[column_name] = None

    raw_df = raw_df[RAW_COLUMNS].copy()
    print(f"[EXTRACT] API success. Rows fetched: {len(raw_df)}")
    return raw_df


def show_data(title, df, columns):
    print(f"\n{'=' * 70}")
    print(title)
    print(f"{'=' * 70}")

    if df.empty:
        print("No data available.")
        return

    selected_columns = [column for column in columns if column in df.columns]
    view_df = df[selected_columns] if selected_columns else df

    print(f"Rows: {len(view_df)} | Columns: {len(view_df.columns)}")
    print(f"Column names: {', '.join(view_df.columns)}")
    print("-" * 70)
    print(view_df.fillna("MISSING").to_string(index=False))


def transform_data(raw_df):
    print("[TRANSFORM] Cleaning and transforming data...")

    df = raw_df.copy()

    df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
    df["repo"] = df["repo"].fillna(DEFAULT_REPO_NAME).astype(str).str.strip()
    df["owner"] = df["owner"].fillna(DEFAULT_OWNER_LOGIN).astype(str).str.strip()
    df["owner_type"] = df["owner_type"].fillna(DEFAULT_OWNER_TYPE).astype(str).str.strip()
    df["language"] = df["language"].fillna(DEFAULT_LANGUAGE).astype(str).str.strip().str.lower()

    df["stars"] = pd.to_numeric(df["stars"], errors="coerce").fillna(DEFAULT_STARS).astype(int)
    df["forks"] = pd.to_numeric(df["forks"], errors="coerce").fillna(DEFAULT_FORKS).astype(int)
    df["issues"] = pd.to_numeric(df["issues"], errors="coerce").fillna(DEFAULT_OPEN_ISSUES).astype(int)
    df["watchers"] = pd.to_numeric(df["watchers"], errors="coerce").fillna(DEFAULT_WATCHERS).astype(int)

    df["private"] = df["private"].fillna(DEFAULT_PRIVATE)
    df["private"] = df["private"].astype(bool)
    df["created_on"] = df["created_on"].fillna(DEFAULT_CREATED_AT).astype(str).str.strip()

    df["owner_lower"] = df["owner"].str.lower()
    df["language_upper"] = df["language"].str.upper()
    df["popularity"] = df["stars"] + df["watchers"]
    df["activity"] = df["forks"] + df["issues"]
    df["cleaned_on"] = utc_now_iso()

    rows_before = len(df)
    df = df.drop_duplicates(subset=["id"], keep="first").reset_index(drop=True)
    print(f"[TRANSFORM] Duplicate rows removed: {rows_before - len(df)}")

    return df


def load_to_sqlite(raw_df, staging_df, run_id):
    print("[LOAD] Loading data into SQLite...")

    with sqlite3.connect(SQLITE_DB_PATH) as connection:
        raw_df[RAW_COLUMNS].to_sql(RAW_TABLE, connection, if_exists="replace", index=False)
        staging_df[STAGING_COLUMNS].to_sql(STAGING_TABLE, connection, if_exists="replace", index=False)

        audit_row = pd.DataFrame(
            [
                {
                    "run_id": run_id,
                    "run_time_utc": utc_now_iso(),
                    "raw_rows": len(raw_df),
                    "staging_rows": len(staging_df),
                    "status": "SUCCESS",
                }
            ]
        )
        audit_row.to_sql(AUDIT_TABLE, connection, if_exists="append", index=False)


def run_pipeline():
    run_id = str(uuid4())

    print("\nStarting GitHub ETL Pipeline")
    print(f"Run ID: {run_id}")
    print(f"SQLite DB: {SQLITE_DB_PATH}")

    raw_df = extract_data()
    show_data("RAW GITHUB DATA", raw_df, RAW_COLUMNS)

    staging_df = transform_data(raw_df)
    show_data("TRANSFORMED GITHUB DATA", staging_df, TRANSFORMED_PREVIEW_COLUMNS)

    load_to_sqlite(raw_df, staging_df, run_id)

    print("\n" + "=" * 70)
    print("ETL SUMMARY")
    print("=" * 70)
    print(f"Raw rows loaded        : {len(raw_df)}")
    print(f"Transformed rows loaded: {len(staging_df)}")
    print(f"Saved tables           : {RAW_TABLE}, {STAGING_TABLE}, {AUDIT_TABLE}")
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
