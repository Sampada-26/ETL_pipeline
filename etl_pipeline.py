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
    print(view_df.to_string(index=False))


def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform raw product data."""
    print("[TRANSFORM] Cleaning and transforming data...")

    stg_df = raw_df.copy()

    # Ensure required columns exist
    required_cols = [
        "id",
        "title",
        "category",
        "brand",
        "price",
        "discountPercentage",
        "rating",
        "stock",
    ]
    for col in required_cols:
        if col not in stg_df.columns:
            stg_df[col] = None

    # Text cleanup
    stg_df["title"] = stg_df["title"].fillna("Unknown Product").astype(str).str.strip()
    stg_df["category"] = stg_df["category"].fillna("uncategorized").astype(str).str.strip().str.lower()
    stg_df["brand"] = stg_df["brand"].fillna("unknown_brand").astype(str).str.strip()

    # Numeric type conversions + missing value handling
    stg_df["price"] = pd.to_numeric(stg_df["price"], errors="coerce").fillna(DEFAULT_PRICE)
    stg_df["discountPercentage"] = (
        pd.to_numeric(stg_df["discountPercentage"], errors="coerce").fillna(DEFAULT_DISCOUNT_PERCENTAGE)
    )
    stg_df["rating"] = pd.to_numeric(stg_df["rating"], errors="coerce").fillna(DEFAULT_RATING)
    stg_df["stock"] = pd.to_numeric(stg_df["stock"], errors="coerce").fillna(DEFAULT_STOCK).astype(int)

    # Unit conversion style transformation: percentage -> ratio
    stg_df["discount_ratio"] = (stg_df["discountPercentage"] / 100).round(3)

    # Final price after discount
    stg_df["final_price"] = (stg_df["price"] * (1 - stg_df["discount_ratio"])).round(2)

    # Normalized fields for analytics
    stg_df["category_normalized"] = stg_df["category"].str.upper()
    stg_df["transformed_at"] = datetime.now(timezone.utc).isoformat()

    return stg_df


def load_to_sqlite(raw_df: pd.DataFrame, stg_df: pd.DataFrame, run_id: str) -> None:
    """Load raw and transformed data into SQLite."""
    print("[LOAD] Loading data into SQLite...")

    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        raw_df.to_sql(RAW_TABLE, conn, if_exists="replace", index=False)
        stg_df.to_sql(STAGING_TABLE, conn, if_exists="replace", index=False)

        audit_row = pd.DataFrame(
            [
                {
                    "run_id": run_id,
                    "run_time_utc": datetime.now(timezone.utc).isoformat(),
                    "raw_rows": len(raw_df),
                    "staging_rows": len(stg_df),
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
        ["id", "title", "category", "brand", "price", "discountPercentage", "rating", "stock"],
    )

    stg_df = transform_data(raw_df)
    show_data(
        "TRANSFORMED DATA (After Transformation)",
        stg_df,
        [
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
        ],
    )

    load_to_sqlite(raw_df, stg_df, run_id)

    print("\n" + "=" * 70)
    print("ETL SUMMARY")
    print("=" * 70)
    print(f"Raw rows loaded        : {len(raw_df)}")
    print(f"Transformed rows loaded: {len(stg_df)}")
    print(f"Saved tables           : {RAW_TABLE}, {STAGING_TABLE}, {AUDIT_TABLE}")
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
