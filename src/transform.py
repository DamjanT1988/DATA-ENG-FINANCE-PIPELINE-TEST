from __future__ import annotations

import logging
from datetime import timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .validate import ACCEPTED_CURRENCIES, normalize_is_refund


logger = logging.getLogger(__name__)

CANONICAL_CATEGORIES = {
    "Groceries",
    "Dining",
    "Transport",
    "Utilities",
    "Entertainment",
    "Shopping",
    "Travel",
    "Healthcare",
    "Income",
    "Fees",
    "Other",
}

_CATEGORY_MAP: Dict[str, str] = {
    # Groceries
    "grocery": "Groceries",
    "groceries": "Groceries",
    "mat": "Groceries",
    # Dining
    "restaurant": "Dining",
    "restaurang": "Dining",
    "dining": "Dining",
    "cafe": "Dining",
    # Transport
    "uber": "Transport",
    "taxi": "Transport",
    "transport": "Transport",
    "transit": "Transport",
    # Utilities
    "utilities": "Utilities",
    "power": "Utilities",
    "internet": "Utilities",
    # Entertainment
    "entertainment": "Entertainment",
    "streaming": "Entertainment",
    # Shopping
    "shopping": "Shopping",
    "electronics": "Shopping",
    "books": "Shopping",
    "book": "Shopping",
    # Travel
    "travel": "Travel",
    "flight": "Travel",
    "hotel": "Travel",
    # Healthcare
    "health": "Healthcare",
    "healthcare": "Healthcare",
    # Income
    "income": "Income",
    "salary": "Income",
    # Fees
    "fees": "Fees",
    "fee": "Fees",
}


def map_category(raw: Any) -> str:
    if raw is None:
        return "Other"
    s = str(raw).strip()
    if not s:
        return "Other"
    key = s.lower()
    return _CATEGORY_MAP.get(key, s.title() if s.title() in CANONICAL_CATEGORIES else "Other")


def _parse_datetime_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True, format="mixed")


def _parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", format="mixed").dt.date


def transform_snapshot(snapshot_csv: Path, processed_dir: Path, run_ts: str) -> Path:
    logger.info("Transforming snapshot: %s", snapshot_csv)
    df = pd.read_csv(snapshot_csv)

    df["currency"] = df["currency"].astype(str).str.upper()
    df["status"] = df["status"].astype(str).str.upper()

    df["is_refund"] = df["is_refund"].map(normalize_is_refund)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").round(2)

    # Parse timestamps/dates
    ts = _parse_datetime_utc(df["transaction_ts"])
    df["transaction_ts"] = ts.dt.tz_convert(timezone.utc)

    posting_date = _parse_date(df["posting_date"])
    # If posting_date is missing/unparseable (allowed in small %), fall back to the transaction date.
    fallback_posting = pd.to_datetime(df["transaction_ts"], errors="coerce", utc=True).dt.date
    df["posting_date"] = posting_date.where(~pd.isna(posting_date), fallback_posting)

    df["category"] = df["category"].map(map_category)

    # Drop rows that would violate the typed staging table / dbt tests.
    before = len(df)
    df = df[df["transaction_id"].astype("string").fillna("").str.strip() != ""]
    df = df[df["account_id"].astype("string").fillna("").str.strip() != ""]
    df = df[df["transaction_ts"].notna()]
    df = df[df["posting_date"].notna()]
    df = df[df["currency"].isin(ACCEPTED_CURRENCIES)]
    df = df[df["is_refund"].isin([True, False])]
    df = df[df["amount"].notna()]
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %s rows during cleaning (staging-safe filter).", dropped)

    # Enforce sign convention (defensive; validate already checks this).
    df.loc[df["is_refund"] == False, "amount"] = df.loc[df["is_refund"] == False, "amount"].abs()  # noqa: E712
    df.loc[df["is_refund"] == True, "amount"] = -df.loc[df["is_refund"] == True, "amount"].abs()  # noqa: E712

    # Dedupe by keeping the latest posting_date (then latest transaction_ts as tie-break)
    df = df.sort_values(["transaction_id", "posting_date", "transaction_ts"], ascending=[True, True, True])
    df = df.drop_duplicates(subset=["transaction_id"], keep="last").reset_index(drop=True)

    # Standardize formats for CSV output
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / f"clean_transactions_{run_ts}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Wrote clean output: %s (rows=%s)", out_path, len(df))
    return out_path


