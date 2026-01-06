from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

from .config import load_config
from .extract import extract_csv
from .load import load_to_postgres
from .logging_config import configure_logging
from .transform import transform_snapshot
from .validate import ValidationError, validate_or_raise


logger = logging.getLogger(__name__)


def run_dbt(project_dir: Path) -> None:
    profiles_dir = Path("/root/.dbt")

    logger.info("Running dbt deps")
    subprocess.run(
        ["dbt", "deps", "--profiles-dir", str(profiles_dir), "--project-dir", str(project_dir)],
        check=True,
    )

    logger.info("Running dbt run")
    subprocess.run(
        ["dbt", "run", "--profiles-dir", str(profiles_dir), "--project-dir", str(project_dir)],
        check=True,
    )

    logger.info("Running dbt test")
    subprocess.run(
        ["dbt", "test", "--profiles-dir", str(profiles_dir), "--project-dir", str(project_dir)],
        check=True,
    )


def main() -> int:
    configure_logging()
    cfg = load_config()

    try:
        extract_res = extract_csv(cfg.paths.raw_input_csv, cfg.paths.processed_dir)
        validate_or_raise(extract_res.snapshot_path, cfg.paths.processed_dir, extract_res.run_ts)
        clean_csv = transform_snapshot(extract_res.snapshot_path, cfg.paths.processed_dir, extract_res.run_ts)
        load_to_postgres(
            cfg.pg,
            schema_sql=cfg.paths.schema_sql,
            raw_snapshot_csv=extract_res.snapshot_path,
            clean_csv=clean_csv,
        )
        run_dbt(cfg.paths.dbt_project_dir)
    except ValidationError:
        return 2
    except subprocess.CalledProcessError as e:
        logger.exception("Subprocess failed: %s", e)
        return 3
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return 1

    logger.info("Pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


