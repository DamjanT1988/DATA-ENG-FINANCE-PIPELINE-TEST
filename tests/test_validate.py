from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.validate import ValidationError, ValidationThresholds, normalize_is_refund, validate_transactions


def _write_csv(df: pd.DataFrame, path: Path) -> Path:
    df.to_csv(path, index=False)
    return path


def test_normalize_is_refund_accepts_common_encodings() -> None:
    assert normalize_is_refund(True) is True
    assert normalize_is_refund(False) is False
    assert normalize_is_refund("1") is True
    assert normalize_is_refund("0") is False
    assert normalize_is_refund("TRUE") is True
    assert normalize_is_refund("FALSE") is False
    assert normalize_is_refund("t") is True
    assert normalize_is_refund("f") is False


def test_validation_threshold_invalid_currency_fails_when_exceeded(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "TXN1",
                "account_id": "ACC1",
                "transaction_ts": "2025-01-01T10:00:00Z",
                "posting_date": "2025-01-01",
                "currency": "XXX",  # invalid
                "amount": "10.00",
                "merchant_id": "M1",
                "merchant_name": "Test",
                "category": "grocery",
                "country": "SE",
                "city": "Stockholm",
                "payment_method": "CARD",
                "status": "BOOKED",
                "is_refund": "0",
                "reference": "r1",
            },
            {
                "transaction_id": "TXN2",
                "account_id": "ACC1",
                "transaction_ts": "2025-01-02T10:00:00Z",
                "posting_date": "2025-01-02",
                "currency": "SEK",
                "amount": "10.00",
                "merchant_id": "M1",
                "merchant_name": "Test",
                "category": "grocery",
                "country": "SE",
                "city": "Stockholm",
                "payment_method": "CARD",
                "status": "BOOKED",
                "is_refund": "0",
                "reference": "r2",
            },
        ]
    )
    csv_path = _write_csv(df, tmp_path / "in.csv")

    report = validate_transactions(
        csv_path,
        thresholds=ValidationThresholds(invalid_currency_pct_max=0.0),
    )
    assert report["passed"] is False
    assert "invalid_currency_threshold_exceeded" in report["failed_checks"]


def test_validation_threshold_unparseable_dates_fails_when_exceeded(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "TXN1",
                "account_id": "ACC1",
                "transaction_ts": "not-a-date",
                "posting_date": "2025-01-01",
                "currency": "SEK",
                "amount": "10.00",
                "merchant_id": "M1",
                "merchant_name": "Test",
                "category": "grocery",
                "country": "SE",
                "city": "Stockholm",
                "payment_method": "CARD",
                "status": "BOOKED",
                "is_refund": "0",
                "reference": "r1",
            },
            {
                "transaction_id": "TXN2",
                "account_id": "ACC1",
                "transaction_ts": "2025-01-02T10:00:00Z",
                "posting_date": "2025-01-02",
                "currency": "SEK",
                "amount": "10.00",
                "merchant_id": "M1",
                "merchant_name": "Test",
                "category": "grocery",
                "country": "SE",
                "city": "Stockholm",
                "payment_method": "CARD",
                "status": "BOOKED",
                "is_refund": "0",
                "reference": "r2",
            },
        ]
    )
    csv_path = _write_csv(df, tmp_path / "in.csv")

    report = validate_transactions(
        csv_path,
        thresholds=ValidationThresholds(unparseable_dates_pct_max=0.0),
    )
    assert report["passed"] is False
    assert "unparseable_dates_threshold_exceeded" in report["failed_checks"]


def test_validation_threshold_duplicates_fails_when_exceeded(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "TXN1",
                "account_id": "ACC1",
                "transaction_ts": "2025-01-01T10:00:00Z",
                "posting_date": "2025-01-01",
                "currency": "SEK",
                "amount": "10.00",
                "merchant_id": "M1",
                "merchant_name": "Test",
                "category": "grocery",
                "country": "SE",
                "city": "Stockholm",
                "payment_method": "CARD",
                "status": "BOOKED",
                "is_refund": "0",
                "reference": "r1",
            },
            {
                "transaction_id": "TXN1",  # duplicate id
                "account_id": "ACC1",
                "transaction_ts": "2025-01-01T11:00:00Z",
                "posting_date": "2025-01-02",
                "currency": "SEK",
                "amount": "11.00",
                "merchant_id": "M1",
                "merchant_name": "Test",
                "category": "grocery",
                "country": "SE",
                "city": "Stockholm",
                "payment_method": "CARD",
                "status": "BOOKED",
                "is_refund": "0",
                "reference": "r2",
            },
        ]
    )
    csv_path = _write_csv(df, tmp_path / "in.csv")

    report = validate_transactions(
        csv_path,
        thresholds=ValidationThresholds(duplicate_transaction_id_pct_max=0.0),
    )
    assert report["passed"] is False
    assert "duplicate_transaction_id_threshold_exceeded" in report["failed_checks"]


