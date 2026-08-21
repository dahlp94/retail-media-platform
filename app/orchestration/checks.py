"""
Source-readiness and publication checks for the measurement pipeline.

These helpers validate that the warehouse and processed artifacts are
structurally usable. They do not recompute treatment effects, SRM, or
campaign decisions.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from app.statistics.experiment_decisions import (
    ALIGNMENT_ALIGNED_POSITIVE,
    ALIGNMENT_ATTRIBUTION_STRONGER,
    ALIGNMENT_INCONCLUSIVE,
    ALIGNMENT_INCREMENTALITY_STRONGER,
    ALIGNMENT_NOT_INTERPRETABLE,
    DECISION_DO_NOT_INTERPRET,
    DECISION_INCONCLUSIVE,
    DECISION_INCREASE_BUDGET,
    DECISION_MAINTAIN,
    DECISION_MONITOR,
    DECISION_REDUCE_BUDGET,
)
from app.statistics.experiment_health import (
    HEALTH_STATUS_FAIL,
    HEALTH_STATUS_PASS,
    HEALTH_STATUS_WARN,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]

REQUIRED_ENV_VARS = (
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
)

REQUIRED_RAW_TABLES = (
    "members",
    "campaigns",
    "campaign_experiment_assignments",
    "ad_events",
    "transactions",
)

REQUIRED_SCRIPTS = (
    "scripts/run_dbt.sh",
    "scripts/run_incrementality.py",
    "scripts/run_experiment_decisions.py",
)

REQUIRED_DBT_FILES = (
    "dbt/dbt_project.yml",
    "dbt/profiles.yml",
)

SYNTHETIC_FILES = (
    "data/synthetic/members.csv",
    "data/synthetic/campaigns.csv",
    "data/synthetic/campaign_experiment_assignments.csv",
    "data/synthetic/ad_events.csv",
    "data/synthetic/transactions.csv",
)

PROCESSED_DIR = REPO_ROOT / "data" / "processed"

ALLOWED_HEALTH_STATUS = frozenset(
    {HEALTH_STATUS_PASS, HEALTH_STATUS_WARN, HEALTH_STATUS_FAIL}
)
ALLOWED_DECISIONS = frozenset(
    {
        DECISION_DO_NOT_INTERPRET,
        DECISION_INCREASE_BUDGET,
        DECISION_MAINTAIN,
        DECISION_MONITOR,
        DECISION_REDUCE_BUDGET,
        DECISION_INCONCLUSIVE,
    }
)
ALLOWED_ALIGNMENT = frozenset(
    {
        ALIGNMENT_ALIGNED_POSITIVE,
        ALIGNMENT_ATTRIBUTION_STRONGER,
        ALIGNMENT_INCREMENTALITY_STRONGER,
        ALIGNMENT_INCONCLUSIVE,
        ALIGNMENT_NOT_INTERPRETABLE,
    }
)

DESIGN_REQUIRED_COLUMNS = (
    "campaign_id",
    "experiment_id",
    "primary_metric",
    "alpha",
)
HEALTH_REQUIRED_COLUMNS = (
    "campaign_id",
    "experiment_health_status",
    "srm_flag",
    "missing_member_outcome_count",
    "control_impressions",
)
DECISION_REQUIRED_COLUMNS = (
    "campaign_id",
    "measurement_decision",
    "attribution_incrementality_alignment",
    "iroas",
    "roas",
    "incremental_revenue",
)


class PipelineCheckError(RuntimeError):
    """Raised when a readiness or publication check fails."""


def _require(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def check_environment(repo_root: Path | None = None) -> dict[str, Any]:
    """
    Confirm env vars and repository files needed to run the pipeline.

    ``DATABASE_URL`` may stand in for discrete ``POSTGRES_*`` variables.
    Passwords are never logged.
    """
    root = repo_root or REPO_ROOT
    errors: list[str] = []
    has_database_url = bool(os.getenv("DATABASE_URL", "").strip())
    missing_env = [name for name in REQUIRED_ENV_VARS if not os.getenv(name, "").strip()]
    if missing_env and not has_database_url:
        errors.append(
            "Missing database configuration: "
            + ", ".join(missing_env)
            + " (or set DATABASE_URL)"
        )
    elif missing_env and has_database_url:
        logger.info(
            "DATABASE_URL is set; discrete POSTGRES_* vars missing: %s",
            ", ".join(missing_env),
        )

    for relative in REQUIRED_SCRIPTS + REQUIRED_DBT_FILES:
        path = root / relative
        _require(path.is_file(), f"Missing required file: {relative}", errors)

    if errors:
        raise PipelineCheckError("; ".join(errors))

    logger.info("Environment check passed. repo_root=%s", root)
    return {"repo_root": str(root), "database_url_configured": has_database_url}


def check_database(engine: Engine) -> dict[str, Any]:
    """Confirm PostgreSQL accepts a connection."""
    with engine.connect() as conn:
        conn.execute(text("select 1"))
    logger.info("Database connection succeeded.")
    return {"ok": True}


def check_source_readiness(
    engine: Engine,
    repo_root: Path | None = None,
    *,
    require_synthetic_files: bool = True,
) -> dict[str, Any]:
    """
    Confirm frozen raw tables exist and are non-empty.

    Synthetic CSVs are checked when present on disk so the DAG fails early
    if the seed-0 snapshot is missing. They are not regenerated.
    """
    root = repo_root or REPO_ROOT
    errors: list[str] = []
    inspector = inspect(engine)
    raw_tables = set(inspector.get_table_names(schema="raw"))
    row_counts: dict[str, int] = {}

    for table in REQUIRED_RAW_TABLES:
        if table not in raw_tables:
            errors.append(f"Missing raw.{table}")
            continue
        with engine.connect() as conn:
            count = int(
                conn.execute(text(f'select count(*) from raw."{table}"')).scalar() or 0
            )
        row_counts[table] = count
        if count <= 0:
            errors.append(f"raw.{table} is empty")

    if require_synthetic_files:
        for relative in SYNTHETIC_FILES:
            path = root / relative
            if not path.is_file():
                errors.append(f"Missing frozen synthetic file: {relative}")
            elif path.stat().st_size <= 0:
                errors.append(f"Empty synthetic file: {relative}")

    if errors:
        raise PipelineCheckError("; ".join(errors))

    logger.info(
        "Source readiness passed. raw_row_counts=%s",
        row_counts,
    )
    return {"raw_row_counts": row_counts}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise PipelineCheckError(f"Missing processed output: {path}")
    if path.stat().st_size <= 0:
        raise PipelineCheckError(f"Empty processed output: {path}")
    return pd.read_csv(path)


def _require_unique_campaigns(df: pd.DataFrame, label: str, errors: list[str]) -> None:
    if "campaign_id" not in df.columns:
        errors.append(f"{label} is missing campaign_id")
        return
    if df.empty:
        errors.append(f"{label} has no rows")
        return
    if df["campaign_id"].duplicated().any():
        errors.append(f"{label} has duplicate campaign_id values")
    if df["campaign_id"].isna().any():
        errors.append(f"{label} has null campaign_id values")


def _require_columns(df: pd.DataFrame, columns: tuple[str, ...], label: str, errors: list[str]) -> None:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        errors.append(f"{label} is missing columns: {', '.join(missing)}")


def validate_processed_outputs(
    processed_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Publication check for Stage 4 processed artifacts.

    Structural only: files, grain, allowed categories, and row-count
    alignment across campaign-level outputs.
    """
    directory = processed_dir or PROCESSED_DIR
    errors: list[str] = []

    design_path = directory / "experiment_design_metadata.csv"
    health_path = directory / "experiment_health_metrics.csv"
    decisions_path = directory / "campaign_measurement_decisions.csv"

    design = _read_csv(design_path)
    health = _read_csv(health_path)
    decisions = _read_csv(decisions_path)

    _require_columns(design, DESIGN_REQUIRED_COLUMNS, "experiment_design_metadata", errors)
    _require_columns(health, HEALTH_REQUIRED_COLUMNS, "experiment_health_metrics", errors)
    _require_columns(decisions, DECISION_REQUIRED_COLUMNS, "campaign_measurement_decisions", errors)

    _require_unique_campaigns(design, "experiment_design_metadata", errors)
    _require_unique_campaigns(health, "experiment_health_metrics", errors)
    _require_unique_campaigns(decisions, "campaign_measurement_decisions", errors)

    if not errors:
        design_ids = set(design["campaign_id"].astype(str))
        health_ids = set(health["campaign_id"].astype(str))
        decision_ids = set(decisions["campaign_id"].astype(str))
        if not (design_ids == health_ids == decision_ids):
            errors.append(
                "campaign_id sets differ across design, health, and decision outputs"
            )
        n = len(decisions)
        if len(health) != n or len(design) != n:
            errors.append(
                "campaign-level output row counts do not match "
                f"(design={len(design)}, health={len(health)}, decisions={n})"
            )
        logger.info("Processed campaign-level grain: %d campaigns", n)

    if "experiment_health_status" in health.columns:
        invalid_health = sorted(
            set(health["experiment_health_status"].dropna().astype(str))
            - ALLOWED_HEALTH_STATUS
        )
        if invalid_health:
            errors.append(
                "Invalid experiment_health_status values: " + ", ".join(invalid_health)
            )

    if "measurement_decision" in decisions.columns:
        invalid_decisions = sorted(
            set(decisions["measurement_decision"].dropna().astype(str))
            - ALLOWED_DECISIONS
        )
        if invalid_decisions:
            errors.append(
                "Invalid measurement_decision values: " + ", ".join(invalid_decisions)
            )

    if "attribution_incrementality_alignment" in decisions.columns:
        invalid_align = sorted(
            set(decisions["attribution_incrementality_alignment"].dropna().astype(str))
            - ALLOWED_ALIGNMENT
        )
        if invalid_align:
            errors.append(
                "Invalid attribution_incrementality_alignment values: "
                + ", ".join(invalid_align)
            )

    if errors:
        raise PipelineCheckError("; ".join(errors))

    summary = {
        "n_campaigns": int(len(decisions)),
        "health_status_counts": health["experiment_health_status"].value_counts().to_dict()
        if "experiment_health_status" in health.columns
        else {},
        "decision_counts": decisions["measurement_decision"].value_counts().to_dict()
        if "measurement_decision" in decisions.columns
        else {},
        "paths": {
            "design": str(design_path),
            "health": str(health_path),
            "decisions": str(decisions_path),
        },
    }
    logger.info(
        "Output validation passed. n_campaigns=%s health=%s decisions=%s",
        summary["n_campaigns"],
        summary["health_status_counts"],
        summary["decision_counts"],
    )
    return summary
