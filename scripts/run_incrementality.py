"""
Build incrementality marts in PostgreSQL and optionally export them to CSV.

After the SQL point-estimate marts are built, campaign-level conversion
inference and member-level bootstrap intervals are attached to
``marts.experiment_lift_metrics``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import yaml
except ImportError:
    yaml = None


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.database import get_engine, get_raw_connection
from app.statistics.experiment_inference import enrich_lift_metrics


SQL_DIR = ROOT / "sql" / "marts"
PROCESSED_DIR = ROOT / "data" / "processed"
SIM_CONFIG_PATH = ROOT / "configs" / "simulation_config.yaml"
EXPERIMENT_CONFIG_PATH = ROOT / "configs" / "experiment_config.yaml"

SQL_MARTS = (
    "experiment_lift_metrics",
    "segment_performance_metrics",
    "experiment_member_outcomes",
)

EXPORT_MARTS = (
    "experiment_lift_metrics",
    "segment_performance_metrics",
)

logger = logging.getLogger(__name__)


def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None or not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_inference_settings(
    experiment_config_path: Path = EXPERIMENT_CONFIG_PATH,
    simulation_config_path: Path = SIM_CONFIG_PATH,
) -> dict[str, Any]:
    """Read confidence level, bootstrap iterations, and RNG seed from YAML."""
    experiment = _load_yaml(experiment_config_path)
    simulation = _load_yaml(simulation_config_path)
    exp_root = experiment.get("experiment", experiment)
    estimation = exp_root.get("estimation", {})
    sim_root = simulation.get("simulation", simulation)
    return {
        "confidence_level": float(estimation.get("confidence_level", 0.95)),
        "bootstrap_iterations": int(estimation.get("bootstrap_iterations", 1000)),
        "random_seed": int(sim_root.get("random_seed", 0)),
    }


def run_marts() -> None:
    """Build incrementality marts from their SQL files."""
    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            for mart in SQL_MARTS:
                sql_path = SQL_DIR / f"{mart}.sql"

                if not sql_path.exists():
                    raise FileNotFoundError(f"SQL file not found: {sql_path}")

                logger.info("Building marts.%s", mart)

                sql_script = sql_path.read_text(encoding="utf-8")
                cur.execute(sql_script)

                logger.info("Built marts.%s", mart)


def enrich_experiment_lift_mart(settings: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Attach analytic and bootstrap uncertainty to campaign lift estimates.

    SQL point estimates are preserved; only uncertainty columns are added.
    The enriched table is written back to ``marts.experiment_lift_metrics``.
    """
    settings = settings or load_inference_settings()
    engine = get_engine()

    lift_df = pd.read_sql_table("experiment_lift_metrics", con=engine, schema="marts")
    member_df = pd.read_sql_table(
        "experiment_member_outcomes",
        con=engine,
        schema="marts",
    )

    logger.info(
        "Estimating uncertainty for %d campaigns "
        "(confidence_level=%s, bootstrap_iterations=%s, random_seed=%s)",
        len(lift_df),
        settings["confidence_level"],
        settings["bootstrap_iterations"],
        settings["random_seed"],
    )

    enriched = enrich_lift_metrics(
        lift_df,
        member_df,
        confidence_level=settings["confidence_level"],
        bootstrap_iterations=settings["bootstrap_iterations"],
        random_seed=settings["random_seed"],
    )

    enriched.to_sql(
        "experiment_lift_metrics",
        con=engine,
        schema="marts",
        index=False,
        if_exists="replace",
    )
    logger.info("Wrote enriched marts.experiment_lift_metrics (%d rows)", len(enriched))
    return enriched


def export_marts() -> None:
    """Export incrementality marts to CSV."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    engine = get_engine()

    for mart in EXPORT_MARTS:
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
        description="Build incrementality marts and campaign-level uncertainty.",
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

    settings = load_inference_settings()
    run_marts()
    enrich_experiment_lift_mart(settings)

    if args.export_csv:
        export_marts()

    logger.info("Incrementality pipeline complete.")


if __name__ == "__main__":
    main()
