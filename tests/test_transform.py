from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.transform import map_category, transform_snapshot


def _write_csv(df: pd.DataFrame, path: Path) -> Path:
    df.to_csv(path, index=False)
    return path


def test_category_mapping_to_canonical_set() -> None:
    assert map_category("GROCERY") == "Groceries"
    assert map_category("restaurant") == "Dining"
    assert map_category("books") == "Shopping"
    assert map_category("") == "Other"
    assert map_category(None) == "Other"


def test_transform_dedupes_by_latest_posting_date(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "TXN1",
                "account_id": "ACC1",
                "transaction_ts": "2025-01-01 10:00:00",
                "posting_date": "2025-01-01",
                "currency": "SEK",
                "amount": "10.00",
                "merchant_id": "M1",
                "merchant_name": "Shop",
                "category": "grocery",
                "country": "SE",
                "city": "Stockholm",
                "payment_method": "CARD",
                "status": "BOOKED",
                "is_refund": "0",
                "reference": "r1",
            },
            {
                "transaction_id": "TXN1",
                "account_id": "ACC1",
                "transaction_ts": "2025-01-01 11:00:00",
                "posting_date": "2025-01-02",
                "currency": "SEK",
                "amount": "11.00",
                "merchant_id": "M1",
                "merchant_name": "Shop",
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
    snapshot = _write_csv(df, tmp_path / "snapshot.csv")
    out = transform_snapshot(snapshot, tmp_path, "TESTTS")
    res = pd.read_csv(out)
    assert len(res) == 1
    assert res.loc[0, "posting_date"] == "2025-01-02"
    assert res.loc[0, "reference"] == "r2"


def test_transform_drops_invalid_currency_rows(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "TXN1",
                "account_id": "ACC1",
                "transaction_ts": "2025-01-01T10:00:00Z",
                "posting_date": "2025-01-01",
                "currency": "XXX",
                "amount": "10.00",
                "merchant_id": "M1",
                "merchant_name": "Shop",
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
                "transaction_ts": "2025-01-01T10:00:00Z",
                "posting_date": "2025-01-01",
                "currency": "SEK",
                "amount": "10.00",
                "merchant_id": "M1",
                "merchant_name": "Shop",
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
    snapshot = _write_csv(df, tmp_path / "snapshot.csv")
    out = transform_snapshot(snapshot, tmp_path, "TESTTS")
    res = pd.read_csv(out)
    assert set(res["transaction_id"].tolist()) == {"TXN2"}


def test_transform_enforces_refund_sign(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "TXN1",
                "account_id": "ACC1",
                "transaction_ts": "2025-01-01T10:00:00Z",
                "posting_date": "2025-01-01",
                "currency": "SEK",
                "amount": "-10.00",  # wrong sign for non-refund
                "merchant_id": "M1",
                "merchant_name": "Shop",
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
                "transaction_ts": "2025-01-01T10:00:00Z",
                "posting_date": "2025-01-01",
                "currency": "SEK",
                "amount": "10.00",  # wrong sign for refund
                "merchant_id": "M1",
                "merchant_name": "Shop",
                "category": "grocery",
                "country": "SE",
                "city": "Stockholm",
                "payment_method": "CARD",
                "status": "BOOKED",
                "is_refund": "1",
                "reference": "r2",
            },
        ]
    )
    snapshot = _write_csv(df, tmp_path / "snapshot.csv")
    out = transform_snapshot(snapshot, tmp_path, "TESTTS")
    res = pd.read_csv(out)
    # Amount column written as numeric-ish string, read back as float.
    amt = dict(zip(res["transaction_id"], res["amount"]))
    assert amt["TXN1"] > 0
    assert amt["TXN2"] < 0


