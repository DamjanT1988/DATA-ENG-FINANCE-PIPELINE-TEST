from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

from .config import PostgresConfig
from .db import connect_with_retries, copy_csv, run_sql_file


logger = logging.getLogger(__name__)

RAW_COLUMNS: Sequence[str] = (
    "transaction_id",
    "account_id",
    "transaction_ts",
    "posting_date",
    "currency",
    "amount",
    "merchant_id",
    "merchant_name",
    "category",
    "country",
    "city",
    "payment_method",
    "status",
    "is_refund",
    "reference",
)

STAGING_COLUMNS: Sequence[str] = (
    "transaction_id",
    "account_id",
    "transaction_ts",
    "posting_date",
    "currency",
    "amount",
    "merchant_id",
    "merchant_name",
    "category",
    "country",
    "city",
    "payment_method",
    "status",
    "is_refund",
    "reference",
)


def load_to_postgres(
    pg: PostgresConfig,
    *,
    schema_sql: Path,
    raw_snapshot_csv: Path,
    clean_csv: Path,
) -> None:
    logger.info("Connecting to Postgres %s:%s/%s", pg.host, pg.port, pg.dbname)
    conn = connect_with_retries(pg)
    try:
        logger.info("Applying warehouse schema: %s", schema_sql)
        run_sql_file(conn, schema_sql)

        logger.info("Loading raw table (append-only): raw.financial_transactions_raw")
        copy_csv(
            conn,
            csv_path=raw_snapshot_csv,
            table_fqn="raw.financial_transactions_raw",
            columns=RAW_COLUMNS,
        )

        logger.info("Refreshing staging table: staging.financial_transactions")
        with conn.cursor() as cur:
            cur.execute("truncate table staging.financial_transactions;")
        conn.commit()

        copy_csv(
            conn,
            csv_path=clean_csv,
            table_fqn="staging.financial_transactions",
            columns=STAGING_COLUMNS,
        )
    finally:
        conn.close()


