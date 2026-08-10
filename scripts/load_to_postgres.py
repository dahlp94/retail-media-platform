"""
Load synthetic CSV files into the PostgreSQL raw schema.

Each CSV in data/synthetic becomes raw.<csv_name>.
Raw columns are stored as TEXT; staging SQL applies types later.
"""

from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path

from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.database import get_raw_connection


DATA_DIR = ROOT / "data" / "synthetic"
SCHEMA = "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)


def read_columns(csv_path: Path) -> list[str]:
    """Read column names from a CSV header."""
    with csv_path.open(newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        return next(reader)


def count_rows(csv_path: Path) -> int:
    """Count CSV data rows, excluding the header."""
    with csv_path.open(encoding="utf-8") as file:
        return max(sum(1 for _ in file) - 1, 0)


def load_csv(csv_path: Path) -> int:
    """Replace raw.<csv_name> with the contents of a CSV file."""
    table = csv_path.stem
    columns = read_columns(csv_path)
    row_count = count_rows(csv_path)

    column_definitions = sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(column))
        for column in columns
    )

    column_names = sql.SQL(", ").join(
        sql.Identifier(column)
        for column in columns
    )

    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(SCHEMA),
                    sql.Identifier(table),
                )
            )

            cur.execute(
                sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(SCHEMA),
                    sql.Identifier(table),
                    column_definitions,
                )
            )

            copy_query = sql.SQL(
                """
                COPY {}.{} ({})
                FROM STDIN
                WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
                """
            ).format(
                sql.Identifier(SCHEMA),
                sql.Identifier(table),
                column_names,
            )

            with csv_path.open("r", encoding="utf-8") as file:
                cur.copy_expert(copy_query.as_string(cur), file)

    logger.info("Loaded %s rows into raw.%s", row_count, table)

    return row_count


def main() -> None:
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(SCHEMA)
                )
            )

    total_rows = sum(load_csv(path) for path in csv_files)

    logger.info(
        "Loaded %s tables and %s total rows.",
        len(csv_files),
        total_rows,
    )


if __name__ == "__main__":
    main()