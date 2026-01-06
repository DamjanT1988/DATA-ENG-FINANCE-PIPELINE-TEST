from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import psycopg2
from psycopg2.extensions import connection as PgConnection

from .config import PostgresConfig


logger = logging.getLogger(__name__)


def connect_with_retries(
    cfg: PostgresConfig,
    *,
    max_attempts: int = 30,
    sleep_seconds: float = 2.0,
) -> PgConnection:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(
                host=cfg.host,
                port=cfg.port,
                dbname=cfg.dbname,
                user=cfg.user,
                password=cfg.password,
            )
            conn.autocommit = False
            return conn
        except Exception as e:  # pragma: no cover (timing-dependent)
            last_err = e
            logger.warning(
                "Postgres connection attempt %s/%s failed: %s",
                attempt,
                max_attempts,
                str(e),
            )
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Could not connect to Postgres after {max_attempts} attempts: {last_err}")


def run_sql_file(conn: PgConnection, sql_file: Path) -> None:
    sql = sql_file.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def copy_csv(
    conn: PgConnection,
    *,
    csv_path: Path,
    table_fqn: str,
    columns: Sequence[str],
) -> None:
    cols = ", ".join(columns)
    sql = f"COPY {table_fqn} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    with conn.cursor() as cur, csv_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(sql=sql, file=f)
    conn.commit()


