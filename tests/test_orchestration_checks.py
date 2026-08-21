"""Tests for pipeline readiness and publication-check helpers."""

from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest

from app.orchestration.checks import (
    PipelineCheckError,
    check_environment,
    validate_processed_outputs,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = REPO_ROOT / "dags" / "retail_media_measurement.py"


def test_dag_file_parses_as_python() -> None:
    source = DAG_FILE.read_text(encoding="utf-8")
    ast.parse(source)
    assert "retail_media_measurement_pipeline" in source
    assert "schedule=None" in source
    assert "catchup=False" in source
    assert "run_dbt_build" in source
    assert "run_incrementality" in source
    assert "run_experiment_decisions" in source
    # Manual Airflow 3 triggers with schedule=None omit logical date.
    assert "ds | default(" in source
    assert "{{{{ ds }}}}" not in source


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("ok\n", encoding="utf-8")


def _stub_repo(tmp_path: Path) -> Path:
    for relative in (
        "scripts/run_dbt.sh",
        "scripts/run_incrementality.py",
        "scripts/run_experiment_decisions.py",
        "dbt/dbt_project.yml",
        "dbt/profiles.yml",
    ):
        _touch(tmp_path / relative)
    return tmp_path


def test_check_environment_requires_database_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_repo(tmp_path)
    for name in (
        "DATABASE_URL",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(PipelineCheckError, match="Missing database configuration"):
        check_environment(tmp_path)


def test_check_environment_accepts_database_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_repo(tmp_path)
    for name in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://retail_media:x@localhost:5432/retail_media")
    result = check_environment(tmp_path)
    assert result["database_url_configured"] is True


def test_check_environment_requires_dbt_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_repo(tmp_path)
    (tmp_path / "dbt" / "dbt_project.yml").unlink()
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_USER", "retail_media")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_DB", "retail_media")
    with pytest.raises(PipelineCheckError, match="dbt/dbt_project.yml"):
        check_environment(tmp_path)


def _write_campaign_outputs(directory: Path, *, n: int = 2, decision: str = "increase_budget") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    campaign_ids = list(range(1, n + 1))
    pd.DataFrame(
        {
            "campaign_id": campaign_ids,
            "experiment_id": campaign_ids,
            "primary_metric": ["incremental_revenue"] * n,
            "alpha": [0.05] * n,
        }
    ).to_csv(directory / "experiment_design_metadata.csv", index=False)
    pd.DataFrame(
        {
            "campaign_id": campaign_ids,
            "experiment_health_status": ["PASS"] * n,
            "srm_flag": ["pass"] * n,
            "missing_member_outcome_count": [0] * n,
            "control_impressions": [0] * n,
        }
    ).to_csv(directory / "experiment_health_metrics.csv", index=False)
    pd.DataFrame(
        {
            "campaign_id": campaign_ids,
            "measurement_decision": [decision] * n,
            "attribution_incrementality_alignment": ["aligned_positive"] * n,
            "iroas": [1.2] * n,
            "roas": [1.1] * n,
            "incremental_revenue": [100.0] * n,
        }
    ).to_csv(directory / "campaign_measurement_decisions.csv", index=False)


def test_validate_processed_outputs_accepts_aligned_campaign_files(tmp_path: Path) -> None:
    _write_campaign_outputs(tmp_path)
    summary = validate_processed_outputs(tmp_path)
    assert summary["n_campaigns"] == 2
    assert summary["decision_counts"]["increase_budget"] == 2


def test_validate_processed_outputs_rejects_missing_file(tmp_path: Path) -> None:
    _write_campaign_outputs(tmp_path)
    (tmp_path / "campaign_measurement_decisions.csv").unlink()
    with pytest.raises(PipelineCheckError, match="Missing processed output"):
        validate_processed_outputs(tmp_path)


def test_validate_processed_outputs_rejects_duplicate_campaign_id(tmp_path: Path) -> None:
    _write_campaign_outputs(tmp_path)
    path = tmp_path / "campaign_measurement_decisions.csv"
    df = pd.read_csv(path)
    df.loc[1, "campaign_id"] = df.loc[0, "campaign_id"]
    df.to_csv(path, index=False)
    with pytest.raises(PipelineCheckError, match="duplicate campaign_id"):
        validate_processed_outputs(tmp_path)


def test_validate_processed_outputs_rejects_unknown_decision(tmp_path: Path) -> None:
    _write_campaign_outputs(tmp_path, decision="scale_forever")
    with pytest.raises(PipelineCheckError, match="Invalid measurement_decision"):
        validate_processed_outputs(tmp_path)


def test_validate_processed_outputs_rejects_mismatched_campaign_sets(tmp_path: Path) -> None:
    _write_campaign_outputs(tmp_path, n=2)
    health = pd.read_csv(tmp_path / "experiment_health_metrics.csv")
    health = health.iloc[[0]]
    health.to_csv(tmp_path / "experiment_health_metrics.csv", index=False)
    with pytest.raises(PipelineCheckError, match="campaign_id sets differ|row counts"):
        validate_processed_outputs(tmp_path)
