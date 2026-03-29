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
except ModuleNotFoundError:
    requests = None

from config import (
    API_URL,
    AUDIT_TABLE,
    DEFAULT_DISCOUNT_PERCENTAGE,
    DEFAULT_PRICE,
    DEFAULT_RATING,
    DEFAULT_STOCK,
    RAW_TABLE,
    REQUEST_TIMEOUT_SECONDS,
    SQLITE_DB_PATH,
    STAGING_TABLE,
)

RAW_COLUMNS = ["id", "title", "category", "brand", "price", "discountPercentage", "rating", "stock"]
TRANSFORMED_PREVIEW_COLUMNS = [
    "id",
    "title",
    "category_normalized",
    "brand",
    "price",
    "discountPercentage",
    "discount_ratio",
    "final_price",
    "rating",
    "stock",
]
STAGING_COLUMNS = [
    "id",
    "title",
    "category",
    "category_normalized",
    "brand",
    "price",
    "discountPercentage",
    "discount_ratio",
    "final_price",
    "rating",
    "stock",
    "transformed_at",
]
TEXT_DEFAULTS = {
    "title": "Unknown Product",
    "category": "uncategorized",
    "brand": "unknown_brand",
}
NUMERIC_DEFAULTS = {
    "price": DEFAULT_PRICE,
    "discountPercentage": DEFAULT_DISCOUNT_PERCENTAGE,
    "rating": DEFAULT_RATING,
    "stock": DEFAULT_STOCK,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fallback_products() -> list[dict]:
    """Local fallback if API is unavailable."""
    return [
        {
            "id": 1,
            "title": "Wireless Mouse",
            "category": "electronics",
            "brand": "LogiTech",
            "price": 899,
            "discountPercentage": 10,
            "rating": 4.4,
            "stock": 120,
        },
        {
            "id": 2,
            "title": "Notebook",
            "category": "stationery",
            "brand": None,
            "price": 60,
            "discountPercentage": None,
            "rating": 4.1,
            "stock": None,
        },
        {
            "id": 3,
            "title": "Coffee Mug",
            "category": None,
            "brand": "HomeBasics",
            "price": None,
            "discountPercentage": 5,
            "rating": None,
            "stock": 80,
        },
    ]


def extract_data() -> pd.DataFrame:
    """Extract products from one API, fallback to local data if needed."""
    print("[EXTRACT] Fetching products from API...")

    if requests is None:
        print("[EXTRACT] `requests` not installed. Using fallback products.")
        return pd.DataFrame(fallback_products())

    try:
        response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        products = payload.get("products", [])
        if not products:
            print("[EXTRACT] API returned 0 rows. Using fallback products.")
            products = fallback_products()
        print(f"[EXTRACT] API success. Rows fetched: {len(products)}")
        return pd.DataFrame(products)
    except Exception as exc:
        print(f"[EXTRACT] API failed ({exc}). Using fallback products.")
        return pd.DataFrame(fallback_products())


def show_data(title: str, df: pd.DataFrame, columns: list[str]) -> None:
    """Show clean stage output using selected columns."""
    print(f"\n{'=' * 70}")
    print(f"{title}")
    print(f"{'=' * 70}")

    if df.empty:
        print("No data available.")
        return

    selected_columns = [col for col in columns if col in df.columns]
    view_df = df[selected_columns] if selected_columns else df
    print(f"Rows: {len(view_df)} | Columns: {len(view_df.columns)}")
    print(f"Column names: {', '.join(view_df.columns)}")
    print("-" * 70)
    print(view_df.fillna("MISSING").to_string(index=False))


def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform raw product data."""
    print("[TRANSFORM] Cleaning and transforming data...")

    df = raw_df.copy()
    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    for col, default in TEXT_DEFAULTS.items():
        df[col] = df[col].fillna(default).astype(str).str.strip()
    df["category"] = df["category"].str.lower()

    for col, default in NUMERIC_DEFAULTS.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
    df["stock"] = df["stock"].astype(int)

    df["discount_ratio"] = (df["discountPercentage"] / 100).round(3)
    df["final_price"] = (df["price"] * (1 - df["discount_ratio"])).round(2)
    df["category_normalized"] = df["category"].str.upper()
    df["transformed_at"] = utc_now_iso()
    return df


def load_to_sqlite(raw_df: pd.DataFrame, staging_df: pd.DataFrame, run_id: str) -> None:
    """Load raw and transformed data into SQLite."""
    print("[LOAD] Loading data into SQLite...")

    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        raw_for_storage = raw_df[[col for col in RAW_COLUMNS if col in raw_df.columns]].copy()
        staging_for_storage = staging_df[[col for col in STAGING_COLUMNS if col in staging_df.columns]].copy()

        raw_for_storage.to_sql(RAW_TABLE, conn, if_exists="replace", index=False)
        staging_for_storage.to_sql(STAGING_TABLE, conn, if_exists="replace", index=False)

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
        audit_row.to_sql(AUDIT_TABLE, conn, if_exists="append", index=False)


def run_pipeline() -> None:
    run_id = str(uuid4())
    print("\nStarting Product ETL Pipeline")
    print(f"Run ID: {run_id}")
    print(f"SQLite DB: {SQLITE_DB_PATH}")

    raw_df = extract_data()
    show_data(
        "RAW DATA (Before Transformation)",
        raw_df,
        RAW_COLUMNS,
    )

    staging_df = transform_data(raw_df)
    show_data(
        "TRANSFORMED DATA (After Transformation)",
        staging_df,
        TRANSFORMED_PREVIEW_COLUMNS,
    )

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
