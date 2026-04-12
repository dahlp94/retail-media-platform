"""
Build campaign-level budget recommendations from incrementality marts.

Reads ``marts.campaign_efficiency_flags`` and ``marts.campaign_incrementality_rankings``
from PostgreSQL (both must list the same campaigns for a row to appear), maps
``efficiency_flag`` to a deterministic ``recommendation`` string, and writes
``data/processed/campaign_recommendations.csv``.

Prerequisites: ``DATABASE_URL`` or ``POSTGRES_*`` env vars (see ``app.core.database``),
and marts built so the two source tables exist and are populated.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.core.database import get_engine  # noqa: E402

logger = logging.getLogger(__name__)

# efficiency_flag value -> budget recommendation (fixed, interview-friendly mapping).
RECOMMENDATION_BY_FLAG: dict[str, str] = {
    "high_impact": "increase_budget",
    "moderate": "maintain",
    "low_impact": "monitor",
    "inefficient": "reduce_budget",
}


def load_mart_tables(engine) -> tuple["object", "object"]:
    """
    Load the two decision-layer marts as DataFrames.

    Parameters
    ----------
    engine
        SQLAlchemy engine from ``get_engine()``.

    Returns
    -------
    tuple[pandas.DataFrame, pandas.DataFrame]
        ``(efficiency_flags, incrementality_rankings)``.
    """
    import pandas as pd

    flags = pd.read_sql_table("campaign_efficiency_flags", engine, schema="marts")
    rankings = pd.read_sql_table("campaign_incrementality_rankings", engine, schema="marts")
    return flags, rankings


def build_recommendations(flags, rankings):
    """
    Inner-join flags and rankings on ``campaign_id`` and attach recommendations.

    Only campaigns present in both tables are included. Output columns:
    ``campaign_id``, ``efficiency_flag``, ``recommendation``.

    Parameters
    ----------
    flags
        DataFrame from ``marts.campaign_efficiency_flags``.
    rankings
        DataFrame from ``marts.campaign_incrementality_rankings``.

    Returns
    -------
    pandas.DataFrame
        One row per campaign with ``recommendation`` filled from ``RECOMMENDATION_BY_FLAG``;
        unknown or missing flags map to ``"unknown"``.
    """
    required_flag_cols = {"campaign_id", "efficiency_flag"}
    missing = required_flag_cols - set(flags.columns)
    if missing:
        raise ValueError(f"campaign_efficiency_flags missing columns: {sorted(missing)}")

    if "campaign_id" not in rankings.columns:
        raise ValueError("campaign_incrementality_rankings must include campaign_id.")

    flags_dedup = flags.drop_duplicates(subset=["campaign_id"], keep="first")
    joined = flags_dedup.merge(
        rankings[["campaign_id"]].drop_duplicates(),
        on="campaign_id",
        how="inner",
        validate="many_to_one",
    )

    out = joined[["campaign_id", "efficiency_flag"]].copy()
    out["recommendation"] = out["efficiency_flag"].map(RECOMMENDATION_BY_FLAG)
    unmapped = out["efficiency_flag"].notna() & out["recommendation"].isna()
    if unmapped.any():
        bad = sorted(out.loc[unmapped, "efficiency_flag"].unique().tolist())
        logger.warning("Unmapped efficiency_flag values (recommendation set to unknown): %s", bad)
    out["recommendation"] = out["recommendation"].fillna("unknown")
    out = out.sort_values("campaign_id", kind="mergesort").reset_index(drop=True)
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Write campaign budget recommendations CSV from marts.efficiency_flags "
        "and marts.campaign_incrementality_rankings.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_ROOT / "data" / "processed" / "campaign_recommendations.csv",
        help="Output CSV path (default: <project>/data/processed/campaign_recommendations.csv).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """
    Generate ``campaign_recommendations.csv`` from PostgreSQL marts.

    Returns
    -------
    int
        ``0`` on success, ``1`` on failure.
    """
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    try:
        engine = get_engine()
        flags, rankings = load_mart_tables(engine)
        out = build_recommendations(flags, rankings)
        out_path = args.output.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(out_path, index=False)
        logger.info("Wrote %d rows to %s.", len(out), out_path)
        return 0
    except Exception:
        logger.exception("generate_recommendations failed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
