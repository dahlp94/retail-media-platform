"""
Execute incrementality mart SQL against PostgreSQL.

Reads ``sql/marts/experiment_lift_metrics.sql`` and
``sql/marts/segment_performance_metrics.sql`` from the project root, runs each
statement in order inside a single transaction, and optionally exports the
resulting ``marts.*`` tables to ``data/processed/*.csv``.

Prerequisites: ``DATABASE_URL`` or ``POSTGRES_*`` env vars (see ``app.core.database``),
and existing raw + staging data so the mart definitions can run successfully.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from app.core.database import get_connection, get_engine  # noqa: E402

logger = logging.getLogger(__name__)

# Relative to project root: ``sql/marts/<name>``.
MART_SQL_FILES: tuple[tuple[str, str], ...] = (
    ("experiment_lift_metrics.sql", "experiment lift metrics"),
    ("segment_performance_metrics.sql", "segment performance metrics"),
)

# Table name (in schema ``marts``) -> output CSV filename under ``data/processed/``.
EXPORT_SPECS: tuple[tuple[str, str], ...] = (
    ("experiment_lift_metrics", "experiment_lift_metrics.csv"),
    ("segment_performance_metrics", "segment_performance_metrics.csv"),
)


def _split_sql_statements(sql: str) -> list[str]:
    """
    Split a SQL script into executable statements.

    Assumes each statement ends at a line whose trailing content is ``...;`` (after
    rstrip). This matches this repository's mart files, including multiline
    ``COMMENT ON`` blocks where only the closing line ends with a semicolon.
    """
    def _has_executable_sql(block: str) -> bool:
        """True if block has at least one non-comment, non-empty line."""
        for raw in block.split("\n"):
            line = raw.strip()
            if not line or line.startswith("--"):
                continue
            return True
        return False

    sql = sql.replace("\r\n", "\n")
    statements: list[str] = []
    buf: list[str] = []
    for line in sql.split("\n"):
        buf.append(line)
        if line.rstrip().endswith(";"):
            block = "\n".join(buf).strip()
            buf = []
            if block and _has_executable_sql(block):
                statements.append(block)
    remainder = "\n".join(buf).strip()
    if remainder and _has_executable_sql(remainder):
        statements.append(remainder)
    return statements


def run_mart_sql_files(sql_dir: Path) -> None:
    """
    Execute each Week 6 mart file in order inside one database transaction.

    Parameters
    ----------
    sql_dir
        Directory containing ``experiment_lift_metrics.sql`` and
        ``segment_performance_metrics.sql`` (default: ``<project>/sql/marts``).
    """
    paths: list[Path] = []
    for filename, _label in MART_SQL_FILES:
        path = sql_dir / filename
        if not path.is_file():
            raise FileNotFoundError(f"SQL file not found: {path}")
        paths.append(path)

    with get_connection() as conn:
        for path, (_fn, label) in zip(paths, MART_SQL_FILES, strict=True):
            logger.info("Running %s…", label)
            raw = path.read_text(encoding="utf-8")
            statements = _split_sql_statements(raw)
            if not statements:
                raise ValueError(f"No SQL statements parsed from {path}")
            for stmt in statements:
                conn.execute(text(stmt))
            logger.info("Finished %s (%d statements).", path.name, len(statements))


def export_marts_to_csv(processed_dir: Path) -> None:
    """
    Snapshot ``marts`` incrementality tables to UTF-8 CSV files (no index column).

    Requires pandas (already in project requirements).
    """
    import pandas as pd

    processed_dir.mkdir(parents=True, exist_ok=True)
    engine = get_engine()

    for table_name, csv_name in EXPORT_SPECS:
        out_path = processed_dir / csv_name
        logger.info("Exporting marts.%s to %s", table_name, out_path)
        df = pd.read_sql_table(table_name, engine, schema="marts")
        df.to_csv(out_path, index=False)
        logger.info("Wrote %d rows to %s.", len(df), csv_name)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build incrementality marts in PostgreSQL and optionally export CSV snapshots.",
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=_ROOT / "sql" / "marts",
        help="Directory containing Week 6 mart SQL files (default: <project>/sql/marts).",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="After building marts, write data/processed/*.csv snapshots.",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=_ROOT / "data" / "processed",
        help="Output directory for CSV exports (default: <project>/data/processed).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging (including per-statement detail).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """
    CLI entrypoint: run incrementality marts and optional CSV export.

    Returns
    -------
    int
        ``0`` on success, ``1`` on failure (after logging the exception).
    """
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    try:
        get_engine()
        run_mart_sql_files(args.sql_dir.resolve())
        logger.info("Incrementality marts refreshed successfully.")
        if args.export_csv:
            export_marts_to_csv(args.processed_dir.resolve())
        return 0
    except Exception:
        logger.exception("run_incrementality failed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
