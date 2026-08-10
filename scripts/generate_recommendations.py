"""
Generate campaign budget recommendations from decision-layer marts.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.database import get_engine


OUTPUT_PATH = ROOT / "data" / "processed" / "campaign_recommendations.csv"

RECOMMENDATIONS = {
    "high_impact": "increase_budget",
    "moderate": "maintain",
    "low_impact": "monitor",
    "inefficient": "reduce_budget",
}

logger = logging.getLogger(__name__)


def build_recommendations() -> pd.DataFrame:
    """Build one recommendation per campaign."""
    engine = get_engine()

    flags = pd.read_sql_table(
        "campaign_efficiency_flags",
        engine,
        schema="marts",
    )

    rankings = pd.read_sql_table(
        "campaign_incrementality_rankings",
        engine,
        schema="marts",
    )

    recommendations = flags[
        ["campaign_id", "efficiency_flag"]
    ].merge(
        rankings[["campaign_id"]],
        on="campaign_id",
        how="inner",
    )

    recommendations["recommendation"] = (
        recommendations["efficiency_flag"]
        .map(RECOMMENDATIONS)
        .fillna("unknown")
    )

    return recommendations.sort_values("campaign_id").reset_index(drop=True)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    recommendations = build_recommendations()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    recommendations.to_csv(OUTPUT_PATH, index=False)

    logger.info(
        "Wrote %d recommendations to %s",
        len(recommendations),
        OUTPUT_PATH,
    )


if __name__ == "__main__":
    main()
