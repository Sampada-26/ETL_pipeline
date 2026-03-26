import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
import requests

from config import (
    API_URL,
    AUDIT_TABLE,
    DEFAULT_AGE_FILL,
    RAW_TABLE,
    REQUEST_TIMEOUT_SECONDS,
    SIMULATE_MISSING_AGE_EVERY_N,
    SQLITE_DB_PATH,
    STAGING_TABLE,
    WAREHOUSE_CITY_TABLE,
    WAREHOUSE_DETAIL_TABLE,
)


def _fallback_users() -> list[dict]:
    """Local fallback data when internet/API is not available."""
    return [
        {
            "id": 1,
            "firstName": "Aarav",
            "lastName": "Patel",
            "email": "aarav.patel@example.com",
            "age": 23,
            "height": 172,
            "weight": 68.0,
            "address": {"city": "Mumbai", "state": "Maharashtra"},
        },
        {
            "id": 2,
            "firstName": "Diya",
            "lastName": "Shah",
            "email": "diya.shah@example.com",
            "age": 29,
            "height": 160,
            "weight": 54.0,
            "address": {"city": "Pune", "state": "Maharashtra"},
        },
        {
            "id": 3,
            "firstName": "Rohan",
            "lastName": "Iyer",
            "email": "rohan.iyer@example.com",
            "age": 41,
            "height": 178,
            "weight": 79.0,
            "address": {"city": "Bengaluru", "state": "Karnataka"},
        },
        {
            "id": 4,
            "firstName": "Anaya",
            "lastName": "Rao",
            "email": "anaya.rao@example.com",
            "age": 35,
            "height": 165,
            "weight": 60.0,
            "address": {"city": "Hyderabad", "state": "Telangana"},
        },
        {
            "id": 5,
            "firstName": "Kabir",
            "lastName": "Singh",
            "email": "kabir.singh@example.com",
            "age": 27,
            "height": 181,
            "weight": 74.0,
            "address": {"city": "Delhi", "state": "Delhi"},
        },
        {
            "id": 6,
            "firstName": "Meera",
            "lastName": "Nair",
            "email": "meera.nair@example.com",
            "age": 32,
            "height": 158,
            "weight": 52.0,
            "address": {"city": "Kochi", "state": "Kerala"},
        },
    ]


def extract_from_api() -> pd.DataFrame:
    """
    Extract user data from API.
    If API is unavailable, use local fallback users.
    """
    extracted_at = datetime.now(timezone.utc).isoformat()

    try:
        response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        users = payload.get("users", [])
        source_system = "dummyjson_api"
        print(f"Extracted {len(users)} rows from API: {API_URL}")
    except Exception as exc:
        users = _fallback_users()
        source_system = "fallback_local_data"
        print(f"API fetch failed ({exc}). Using local fallback with {len(users)} rows.")

    records = []
    for idx, user in enumerate(users, start=1):
        first_name = user.get("firstName", "").strip()
        last_name = user.get("lastName", "").strip()
        full_name = f"{first_name} {last_name}".strip()

        age = user.get("age")
        if SIMULATE_MISSING_AGE_EVERY_N > 0 and idx % SIMULATE_MISSING_AGE_EVERY_N == 0:
            # Intentionally create missing values for ETL practice.
            age = None

        height_cm = user.get("height")
        height_mm_text = None
        if height_cm is not None:
            # Make height as a string (datatype issue) and in mm for conversion task.
            height_mm_text = str(int(float(height_cm) * 10))

        address = user.get("address") or {}
        city = address.get("city") or "Unknown"
        state = address.get("state") or "Unknown"

        records.append(
            {
                "user_id": user.get("id"),
                "full_name": full_name or "Unknown User",
                "email": user.get("email"),
                "age": age,
                "height_mm": height_mm_text,
                "weight_kg": user.get("weight"),
                "city": city,
                "state": state,
                "source_system": source_system,
                "extracted_at": extracted_at,
            }
        )

    return pd.DataFrame(records)


def transform_data(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Transformations done:
    1) Datatype change (string -> numeric)
    2) mm -> cm conversion
    3) Missing age fill
    4) Data manipulation (new columns and grouping)
    """
    stg_df = raw_df.copy()

    # Datatype fixes
    stg_df["age"] = pd.to_numeric(stg_df["age"], errors="coerce")
    stg_df["height_mm"] = pd.to_numeric(stg_df["height_mm"], errors="coerce")
    stg_df["weight_kg"] = pd.to_numeric(stg_df["weight_kg"], errors="coerce")

    # Fill missing age values with median age (or default)
    age_median = stg_df["age"].median()
    age_fill_value = int(round(age_median)) if pd.notna(age_median) else DEFAULT_AGE_FILL
    stg_df["age"] = stg_df["age"].fillna(age_fill_value).astype(int)

    # mm -> cm conversion
    stg_df["height_cm"] = (stg_df["height_mm"] / 10).round(1)

    # Small manipulations
    stg_df["city"] = stg_df["city"].fillna("Unknown").str.strip().str.upper()
    stg_df["email_domain"] = stg_df["email"].fillna("unknown@example.com").str.split("@").str[-1]
    stg_df["bmi"] = (stg_df["weight_kg"] / ((stg_df["height_cm"] / 100) ** 2)).round(2)
    stg_df["age_group"] = pd.cut(
        stg_df["age"],
        bins=[0, 18, 30, 45, 60, 120],
        labels=["Child", "Young Adult", "Adult", "Mid Age", "Senior"],
        include_lowest=True,
    ).astype(str)
    stg_df["transformed_at"] = datetime.now(timezone.utc).isoformat()

    # Data warehouse detail table
    dw_detail_df = stg_df[
        [
            "user_id",
            "full_name",
            "email",
            "email_domain",
            "age",
            "age_group",
            "height_cm",
            "weight_kg",
            "bmi",
            "city",
            "state",
            "source_system",
            "transformed_at",
        ]
    ].copy()

    # Data warehouse aggregate table
    dw_city_df = (
        dw_detail_df.groupby("city", as_index=False)
        .agg(
            users_count=("user_id", "count"),
            avg_age=("age", "mean"),
            avg_height_cm=("height_cm", "mean"),
            avg_bmi=("bmi", "mean"),
        )
        .round({"avg_age": 1, "avg_height_cm": 1, "avg_bmi": 2})
    )

    return stg_df, dw_detail_df, dw_city_df


def load_to_sqlite(
    raw_df: pd.DataFrame,
    stg_df: pd.DataFrame,
    dw_detail_df: pd.DataFrame,
    dw_city_df: pd.DataFrame,
    run_id: str,
) -> None:
    """Load raw, staging and warehouse tables into SQLite."""
    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        raw_df.to_sql(RAW_TABLE, conn, if_exists="replace", index=False)
        stg_df.to_sql(STAGING_TABLE, conn, if_exists="replace", index=False)
        dw_detail_df.to_sql(WAREHOUSE_DETAIL_TABLE, conn, if_exists="replace", index=False)
        dw_city_df.to_sql(WAREHOUSE_CITY_TABLE, conn, if_exists="replace", index=False)

        audit_row = pd.DataFrame(
            [
                {
                    "run_id": run_id,
                    "run_time_utc": datetime.now(timezone.utc).isoformat(),
                    "raw_rows": len(raw_df),
                    "staging_rows": len(stg_df),
                    "warehouse_rows": len(dw_detail_df),
                    "status": "SUCCESS",
                }
            ]
        )
        audit_row.to_sql(AUDIT_TABLE, conn, if_exists="append", index=False)


def simulate_amazon_etl(run_id: str) -> None:
    """
    Simple simulation of an AWS-style ETL flow:
    S3 landing (raw) -> AWS Glue transform (staging) -> Redshift warehouse (dw).
    """
    print("\nSimulated Amazon-style ETL flow")
    print(f"Run ID: {run_id}")
    print(f"1) Landing Zone (S3 equivalent): `{RAW_TABLE}`")
    print(f"2) Glue Transform Job (cleaning rules): `{STAGING_TABLE}`")
    print(f"3) Redshift Warehouse (analytics): `{WAREHOUSE_DETAIL_TABLE}`, `{WAREHOUSE_CITY_TABLE}`")
    print(f"4) CloudWatch Logs equivalent: `{AUDIT_TABLE}`")


def run_pipeline() -> None:
    run_id = str(uuid4())
    print("Starting ETL pipeline...")
    print(f"SQLite DB: {SQLITE_DB_PATH}")

    raw_df = extract_from_api()
    stg_df, dw_detail_df, dw_city_df = transform_data(raw_df)
    load_to_sqlite(raw_df, stg_df, dw_detail_df, dw_city_df, run_id)
    simulate_amazon_etl(run_id)

    print("\nPipeline completed successfully.")
    print(f"Raw rows: {len(raw_df)}")
    print(f"Warehouse detail rows: {len(dw_detail_df)}")
    print(f"Warehouse city rows: {len(dw_city_df)}")
    print("\nSample warehouse data:")
    print(dw_detail_df.head(5).to_string(index=False))


if __name__ == "__main__":
    run_pipeline()
