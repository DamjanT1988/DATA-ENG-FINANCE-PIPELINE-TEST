from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


logger = logging.getLogger(__name__)

ACCEPTED_CURRENCIES = {"SEK", "EUR", "USD", "GBP", "NOK", "DKK"}
ACCEPTED_STATUSES = {"BOOKED", "PENDING", "FAILED"}


class ValidationError(RuntimeError):
    pass


def normalize_is_refund(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    mapping = {
        "1": True,
        "0": False,
        "true": True,
        "false": False,
        "t": True,
        "f": False,
        "yes": True,
        "no": False,
    }
    return mapping.get(s)


@dataclass(frozen=True)
class ValidationThresholds:
    invalid_currency_pct_max: float = 0.01
    unparseable_dates_pct_max: float = 0.005
    duplicate_transaction_id_pct_max: float = 0.005


def _pct(n: int, d: int) -> float:
    return 0.0 if d == 0 else n / d


def _missing_required_str(series: pd.Series) -> int:
    s = series.astype("string")
    return int(s.isna().sum() + (s.str.strip() == "").sum())


def _parse_datetime_utc(series: pd.Series) -> pd.Series:
    # Accept multiple common formats; coerce failures to NaT.
    return pd.to_datetime(series, errors="coerce", utc=True, format="mixed")


def _parse_date(series: pd.Series) -> pd.Series:
    # Parse into date (no time component). Supports mixed formats, coerces failures.
    return pd.to_datetime(series, errors="coerce", format="mixed").dt.date


def validate_transactions(
    csv_path: Path,
    *,
    thresholds: ValidationThresholds = ValidationThresholds(),
) -> Dict[str, Any]:
    df = pd.read_csv(csv_path)
    row_count = int(len(df))

    # Critical not-null checks
    missing_transaction_id = _missing_required_str(df["transaction_id"])
    missing_account_id = _missing_required_str(df["account_id"])

    # Parseability checks
    transaction_ts_parsed = _parse_datetime_utc(df["transaction_ts"])
    unparseable_ts = int(transaction_ts_parsed.isna().sum())
    posting_date_parsed = _parse_date(df["posting_date"])
    unparseable_posting_date = int(pd.isna(posting_date_parsed).sum())
    unparseable_any_date = int(((transaction_ts_parsed.isna()) | (pd.isna(posting_date_parsed))).sum())

    # Accepted values checks
    currency_norm = df["currency"].astype(str).str.upper()
    invalid_currency = int((~currency_norm.isin(ACCEPTED_CURRENCIES)).sum())

    status_norm = df["status"].astype(str).str.upper()
    invalid_status = int((~status_norm.isin(ACCEPTED_STATUSES)).sum())

    is_refund_norm = df["is_refund"].map(normalize_is_refund)
    invalid_is_refund = int(is_refund_norm.isna().sum())

    amount = pd.to_numeric(df["amount"], errors="coerce")
    invalid_amount = int(amount.isna().sum())

    refund_sign_mismatch = int(
        (
            ((is_refund_norm == False) & (amount <= 0))  # noqa: E712
            | ((is_refund_norm == True) & (amount >= 0))  # noqa: E712
        ).sum()
    )

    duplicate_transaction_id = int(df["transaction_id"].duplicated().sum())

    report: Dict[str, Any] = {
        "file": str(csv_path),
        "row_count": row_count,
        "checks": {
            "missing_transaction_id": missing_transaction_id,
            "missing_account_id": missing_account_id,
            "unparseable_transaction_ts": unparseable_ts,
            "unparseable_posting_date": unparseable_posting_date,
            "unparseable_any_date": unparseable_any_date,
            "invalid_currency": invalid_currency,
            "invalid_status": invalid_status,
            "invalid_is_refund": invalid_is_refund,
            "invalid_amount": invalid_amount,
            "refund_sign_mismatch": refund_sign_mismatch,
            "duplicate_transaction_id": duplicate_transaction_id,
        },
        "thresholds": {
            "invalid_currency_pct_max": thresholds.invalid_currency_pct_max,
            "unparseable_dates_pct_max": thresholds.unparseable_dates_pct_max,
            "duplicate_transaction_id_pct_max": thresholds.duplicate_transaction_id_pct_max,
        },
        "pct": {
            "invalid_currency": _pct(invalid_currency, row_count),
            "unparseable_any_date": _pct(unparseable_any_date, row_count),
            "duplicate_transaction_id": _pct(duplicate_transaction_id, row_count),
        },
    }

    failures: list[str] = []
    if missing_transaction_id > 0:
        failures.append("transaction_id_not_null")
    if missing_account_id > 0:
        failures.append("account_id_not_null")
    if invalid_is_refund > 0:
        failures.append("is_refund_normalizable")
    if invalid_amount > 0:
        failures.append("amount_parseable")
    if refund_sign_mismatch > 0:
        failures.append("amount_sign_matches_is_refund")
    if invalid_status > 0:
        failures.append("status_accepted_values")

    # Threshold-based failures
    if _pct(invalid_currency, row_count) > thresholds.invalid_currency_pct_max:
        failures.append("invalid_currency_threshold_exceeded")
    if _pct(unparseable_any_date, row_count) > thresholds.unparseable_dates_pct_max:
        failures.append("unparseable_dates_threshold_exceeded")
    if _pct(duplicate_transaction_id, row_count) > thresholds.duplicate_transaction_id_pct_max:
        failures.append("duplicate_transaction_id_threshold_exceeded")

    report["failed_checks"] = failures
    report["passed"] = len(failures) == 0
    return report


def write_validation_report(report: Dict[str, Any], processed_dir: Path, run_ts: str) -> Path:
    processed_dir.mkdir(parents=True, exist_ok=True)
    path = processed_dir / f"validation_report_{run_ts}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
    return path


def validate_or_raise(csv_path: Path, processed_dir: Path, run_ts: str) -> Path:
    logger.info("Validating snapshot: %s", csv_path)
    report = validate_transactions(csv_path)
    report_path = write_validation_report(report, processed_dir, run_ts)

    if not report.get("passed", False):
        msg = f"Validation failed. See report: {report_path}"
        logger.error(msg)
        raise ValidationError(msg)

    logger.info("Validation passed. Report: %s", report_path)
    return report_path


