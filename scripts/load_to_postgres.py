"""
Load synthetic CSV files from ``data/synthetic/`` into raw PostgreSQL tables.

Table names match CSV stems (e.g. ``members.csv`` → ``raw.members``). All columns
are stored as ``TEXT`` in the raw layer; staging SQL will apply typing later.
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

# Project root on ``sys.path`` so ``app`` imports work without installing the package.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.core.database import get_engine, get_raw_connection  # noqa: E402

logger = logging.getLogger(__name__)

# Only filenames we consider safe as PostgreSQL identifiers (plus .csv).
_SAFE_STEM = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> str:
    if not _SAFE_STEM.match(name):
        raise ValueError(f"Unsafe table/column identifier: {name!r}")
    return name


def _pg_quote_ident(name: str) -> str:
    """Double-quote a PostgreSQL identifier (handles reserved words like ``timestamp``)."""
    _validate_identifier(name)
    return '"' + name.replace('"', '""') + '"'


def _csv_columns(csv_path: Path) -> list[str]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    return [_validate_identifier(h.strip()) for h in header]


def ensure_schema(conn, schema: str) -> None:
    """Create the schema if it does not exist."""
    _validate_identifier(schema)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


def create_raw_table_sql(schema: str, table: str, columns: list[str]) -> str:
    """Build ``CREATE TABLE`` for a raw table with all ``TEXT`` columns."""
    _validate_identifier(schema)
    _validate_identifier(table)
    quoted = [_pg_quote_ident(c) for c in columns]
    col_defs = ", ".join(f"{q} TEXT" for q in quoted)
    return f"CREATE TABLE {schema}.{table} ({col_defs});"


def load_csv_to_table(
    csv_path: Path,
    *,
    schema: str,
    table: str,
) -> int:
    """
    Replace ``schema.table`` with contents of ``csv_path`` via ``COPY``.

    Returns the number of data rows copied (excluding header).
    """
    columns = _csv_columns(csv_path)
    with csv_path.open(encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1

    if row_count < 0:
        raise ValueError(f"CSV has no header row: {csv_path}")

    ddl = create_raw_table_sql(schema, table, columns)
    col_list = ", ".join(_pg_quote_ident(c) for c in columns)
    copy_sql = (
        f"COPY {schema}.{table} ({col_list}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"
    )

    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")
            cur.execute(ddl)
            with csv_path.open("rb") as f:
                cur.copy_expert(copy_sql, f)

    logger.info("Loaded %s rows into %s.%s from %s", row_count, schema, table, csv_path.name)
    return row_count


def discover_csvs(data_dir: Path) -> list[Path]:
    """Return sorted ``*.csv`` paths under ``data_dir``."""
    paths = sorted(data_dir.glob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    return paths


def load_all(
    data_dir: Path,
    *,
    schema: str = "raw",
) -> dict[str, int]:
    """
    Load every ``*.csv`` in ``data_dir`` into ``schema.<stem>``.

    Returns a mapping of table name to row count.
    """
    results: dict[str, int] = {}

    with get_raw_connection() as conn:
        ensure_schema(conn, schema)

    for path in discover_csvs(data_dir):
        stem = path.stem
        _validate_identifier(stem)
        results[stem] = load_csv_to_table(path, schema=schema, table=stem)

    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load synthetic CSVs from data/synthetic into raw PostgreSQL tables.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=_ROOT / "data" / "synthetic",
        help="Directory containing CSV files (default: <project>/data/synthetic).",
    )
    parser.add_argument(
        "--schema",
        default="raw",
        help="PostgreSQL schema for loaded tables (default: raw).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for loading all synthetic CSVs."""
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    # Initialize engine early so connection/config issues fail fast before loading begins.
    get_engine()

    summary = load_all(args.data_dir, schema=args.schema)
    total_rows = sum(summary.values())
    logger.info("Done: %d tables, %d total data rows.", len(summary), total_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
