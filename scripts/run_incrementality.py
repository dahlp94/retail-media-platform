"""
Build incrementality marts in PostgreSQL and optionally export them to CSV.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.database import get_engine, get_raw_connection


SQL_DIR = ROOT / "sql" / "marts"
PROCESSED_DIR = ROOT / "data" / "processed"

MARTS = (
    "experiment_lift_metrics",
    "segment_performance_metrics",
)

logger = logging.getLogger(__name__)


def run_marts() -> None:
    """Build incrementality marts from their SQL files."""
    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            for mart in MARTS:
                sql_path = SQL_DIR / f"{mart}.sql"

                if not sql_path.exists():
                    raise FileNotFoundError(f"SQL file not found: {sql_path}")

                logger.info("Building marts.%s", mart)

                sql_script = sql_path.read_text(encoding="utf-8")
                cur.execute(sql_script)

                logger.info("Built marts.%s", mart)


def export_marts() -> None:
    """Export incrementality marts to CSV."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    engine = get_engine()

    for mart in MARTS:
        output_path = PROCESSED_DIR / f"{mart}.csv"

        df = pd.read_sql_table(
            mart,
            con=engine,
            schema="marts",
        )

        df.to_csv(output_path, index=False)

        logger.info(
            "Exported %d rows to %s",
            len(df),
            output_path,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build incrementality marts.",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export refreshed marts to data/processed.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    run_marts()

    if args.export_csv:
        export_marts()

    logger.info("Incrementality pipeline complete.")


if __name__ == "__main__":
    main()