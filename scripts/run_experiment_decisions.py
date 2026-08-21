"""
Build experiment-health diagnostics and campaign measurement decisions.

Requires dbt-built incrementality, spend, and efficiency marts. Run
``scripts/run_dbt.sh run`` and ``scripts/run_incrementality.py`` first.

Does not regenerate synthetic source data and does not recompute
treatment-effect point estimates. Use ``--legacy-sql`` only to execute
the frozen files under sql/marts/.
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
from app.statistics.experiment_decisions import build_campaign_decisions
from app.statistics.experiment_health import enrich_health_metrics


SQL_DIR = ROOT / "sql" / "marts"
PROCESSED_DIR = ROOT / "data" / "processed"
EXPERIMENT_CONFIG_PATH = ROOT / "configs" / "experiment_config.yaml"

SQL_MARTS = (
    "experiment_design_metadata",
    "experiment_health_metrics",
)

EXPORT_TABLES = (
    "experiment_design_metadata",
    "experiment_health_metrics",
    "campaign_measurement_decisions",
)

logger = logging.getLogger(__name__)


def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None or not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_decision_settings(
    experiment_config_path: Path = EXPERIMENT_CONFIG_PATH,
) -> dict[str, Any]:
    experiment = _load_yaml(experiment_config_path)
    exp_root = experiment.get("experiment", experiment)
    health = exp_root.get("health", {})
    decision = exp_root.get("decision", {})
    return {
        "srm_alpha_fail": float(health.get("srm_alpha_fail", 0.001)),
        "srm_alpha_warn": float(health.get("srm_alpha_warn", 0.05)),
        "balance_smd_warn": float(health.get("balance_smd_warn", 0.10)),
        "scale_min_iroas": float(decision.get("scale_min_iroas", 1.0)),
    }


def run_legacy_sql_marts() -> None:
    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            for mart in SQL_MARTS:
                sql_path = SQL_DIR / f"{mart}.sql"
                if not sql_path.exists():
                    raise FileNotFoundError(f"SQL file not found: {sql_path}")
                logger.info("Building marts.%s", mart)
                cur.execute(sql_path.read_text(encoding="utf-8"))
                logger.info("Built marts.%s", mart)


def enrich_health_mart(settings: dict[str, Any]) -> pd.DataFrame:
    engine = get_engine()
    health_df = pd.read_sql_table(
        "experiment_health_metrics",
        con=engine,
        schema="marts",
    )
    enriched = enrich_health_metrics(
        health_df,
        srm_alpha_fail=settings["srm_alpha_fail"],
        srm_alpha_warn=settings["srm_alpha_warn"],
        balance_smd_warn=settings["balance_smd_warn"],
    )
    enriched.to_sql(
        "experiment_health_metrics",
        con=engine,
        schema="marts",
        index=False,
        if_exists="replace",
    )
    logger.info("Wrote enriched marts.experiment_health_metrics (%d rows)", len(enriched))
    return enriched


def build_decision_mart(settings: dict[str, Any]) -> pd.DataFrame:
    engine = get_engine()
    lift_df = pd.read_sql_table("experiment_lift_metrics", con=engine, schema="marts")
    spend_df = pd.read_sql_table("campaign_spend_metrics", con=engine, schema="marts")
    health_df = pd.read_sql_table(
        "experiment_health_metrics",
        con=engine,
        schema="marts",
    )
    flags_df = pd.read_sql_table(
        "campaign_efficiency_flags",
        con=engine,
        schema="marts",
    )
    decisions = build_campaign_decisions(
        lift_df,
        spend_df,
        health_df,
        flags_df,
        scale_min_iroas=settings["scale_min_iroas"],
    )
    decisions.to_sql(
        "campaign_measurement_decisions",
        con=engine,
        schema="marts",
        index=False,
        if_exists="replace",
    )
    logger.info(
        "Wrote marts.campaign_measurement_decisions (%d rows)",
        len(decisions),
    )
    return decisions


def export_tables() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    engine = get_engine()
    for table in EXPORT_TABLES:
        output_path = PROCESSED_DIR / f"{table}.csv"
        df = pd.read_sql_table(table, con=engine, schema="marts")
        df.to_csv(output_path, index=False)
        logger.info("Exported %d rows to %s", len(df), output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build experiment health diagnostics and measurement decisions.",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export health, design metadata, and decisions to data/processed.",
    )
    parser.add_argument(
        "--legacy-sql",
        action="store_true",
        help="Rebuild design/health SQL from frozen sql/marts/*.sql instead of using dbt output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    settings = load_decision_settings()
    if args.legacy_sql:
        logger.warning("Using frozen sql/marts/ files; prefer `scripts/run_dbt.sh run`.")
        run_legacy_sql_marts()
    else:
        logger.info(
            "Expecting dbt-built marts.experiment_health_metrics and "
            "marts.experiment_design_metadata"
        )
    enrich_health_mart(settings)
    build_decision_mart(settings)
    if args.export_csv:
        export_tables()
    logger.info("Experiment decision pipeline complete.")


if __name__ == "__main__":
    main()
