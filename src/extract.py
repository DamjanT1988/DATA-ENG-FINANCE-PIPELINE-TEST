from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import shutil


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExtractResult:
    run_ts: str
    snapshot_path: Path


def _run_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def extract_csv(raw_input_csv: Path, processed_dir: Path) -> ExtractResult:
    if not raw_input_csv.exists():
        raise FileNotFoundError(f"Raw input CSV not found: {raw_input_csv}")

    processed_dir.mkdir(parents=True, exist_ok=True)
    run_ts = _run_ts()
    snapshot_path = processed_dir / f"raw_snapshot_{run_ts}.csv"

    logger.info("Extracting raw CSV snapshot: %s -> %s", raw_input_csv, snapshot_path)
    shutil.copyfile(raw_input_csv, snapshot_path)
    return ExtractResult(run_ts=run_ts, snapshot_path=snapshot_path)


