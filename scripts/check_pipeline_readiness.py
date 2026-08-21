"""
Validate environment, database connectivity, and frozen source readiness.

Does not regenerate synthetic data. Used by the Airflow DAG and for
manual preflight checks.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.database import get_engine
from app.orchestration.checks import (
    PipelineCheckError,
    check_database,
    check_environment,
    check_source_readiness,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check pipeline environment, database, and raw sources.",
    )
    parser.add_argument(
        "--skip-database",
        action="store_true",
        help="Only check env vars and repository files.",
    )
    parser.add_argument(
        "--skip-sources",
        action="store_true",
        help="Skip raw-table and synthetic-file checks.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    try:
        check_environment()
        if not args.skip_database:
            engine = get_engine()
            check_database(engine)
            if not args.skip_sources:
                check_source_readiness(engine)
    except PipelineCheckError as exc:
        logging.error("Pipeline readiness failed: %s", exc)
        raise SystemExit(1) from exc
    logging.info("Pipeline readiness checks passed.")


if __name__ == "__main__":
    main()
