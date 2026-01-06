from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class PathsConfig:
    project_root: Path
    raw_input_csv: Path
    processed_dir: Path
    schema_sql: Path
    dbt_project_dir: Path


@dataclass(frozen=True)
class AppConfig:
    pg: PostgresConfig
    paths: PathsConfig


def _project_root() -> Path:
    # /app/src/config.py -> /app
    return Path(__file__).resolve().parents[1]


def _parse_database_url(database_url: str) -> Optional[PostgresConfig]:
    if not database_url:
        return None

    u = urlparse(database_url)
    if u.scheme not in {"postgres", "postgresql"}:
        raise ValueError("DATABASE_URL must start with postgresql:// or postgres://")
    if not u.hostname or not u.username or u.password is None or not u.path:
        raise ValueError("DATABASE_URL is missing required components")

    return PostgresConfig(
        host=u.hostname,
        port=int(u.port or 5432),
        dbname=u.path.lstrip("/"),
        user=u.username,
        password=u.password,
    )


def load_config() -> AppConfig:
    root = _project_root()

    database_url = os.getenv("DATABASE_URL", "").strip()
    pg = _parse_database_url(database_url) or PostgresConfig(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "finance_dw"),
        user=os.getenv("POSTGRES_USER", "finance"),
        password=os.getenv("POSTGRES_PASSWORD", "finance_password"),
    )

    paths = PathsConfig(
        project_root=root,
        raw_input_csv=root / "data" / "raw" / "financial_transactions.csv",
        processed_dir=root / "data" / "processed",
        schema_sql=root / "sql" / "schema.sql",
        dbt_project_dir=root / "dbt",
    )
    return AppConfig(pg=pg, paths=paths)


