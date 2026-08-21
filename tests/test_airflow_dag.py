"""Airflow DAG import and structure tests for the measurement pipeline."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

_AIRFLOW_HOME = Path(tempfile.mkdtemp(prefix="rmp_airflow_home_"))
os.environ.setdefault("AIRFLOW_HOME", str(_AIRFLOW_HOME))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

airflow = pytest.importorskip("airflow")
if not getattr(airflow, "__version__", None):
    pytest.skip(
        "airflow namespace is present but apache-airflow is not installed",
        allow_module_level=True,
    )

from airflow.models.dagbag import DagBag

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_FOLDER = REPO_ROOT / "dags"
DAG_ID = "retail_media_measurement_pipeline"

EXPECTED_TASKS = (
    "check_environment",
    "check_database",
    "validate_sources",
    "run_dbt_build",
    "run_dbt_tests",
    "run_incrementality",
    "run_experiment_decisions",
    "validate_outputs",
)

EXPECTED_EDGES = (
    ("check_environment", "check_database"),
    ("check_database", "validate_sources"),
    ("validate_sources", "run_dbt_build"),
    ("run_dbt_build", "run_dbt_tests"),
    ("run_dbt_tests", "run_incrementality"),
    ("run_incrementality", "run_experiment_decisions"),
    ("run_experiment_decisions", "validate_outputs"),
)


@pytest.fixture(scope="module")
def dagbag(tmp_path_factory: pytest.TempPathFactory) -> DagBag:
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    os.environ["AIRFLOW_HOME"] = str(airflow_home)
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "ERROR"
    bag = DagBag(
        dag_folder=str(DAGS_FOLDER),
        include_examples=False,
    )
    return bag


@pytest.fixture(scope="module")
def dag(dagbag: DagBag):
    assert dagbag.import_errors == {}, dagbag.import_errors
    loaded = dagbag.dags.get(DAG_ID)
    assert loaded is not None
    return loaded


def test_dag_imports_without_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}
    assert DAG_ID in dagbag.dags


def test_dag_has_expected_tasks(dag) -> None:
    assert set(dag.task_ids) == set(EXPECTED_TASKS)


def test_dag_has_no_cycles(dag) -> None:
    dag.check_cycle()


def test_task_dependency_order(dag) -> None:
    for upstream, downstream in EXPECTED_EDGES:
        assert downstream in dag.get_task(upstream).downstream_task_ids
        assert upstream in dag.get_task(downstream).upstream_task_ids


def test_dbt_runs_before_inference(dag) -> None:
    infer = dag.get_task("run_incrementality")
    assert "run_dbt_tests" in infer.upstream_task_ids
    assert "run_dbt_build" in dag.get_task("run_dbt_tests").upstream_task_ids


def test_incrementality_runs_before_decisions(dag) -> None:
    assert "run_incrementality" in dag.get_task("run_experiment_decisions").upstream_task_ids


def test_dbt_test_failure_blocks_python_enrichment(dag) -> None:
    assert "run_incrementality" in dag.get_task("run_dbt_tests").downstream_task_ids
    assert "run_experiment_decisions" not in dag.get_task("run_dbt_tests").upstream_task_ids


def test_schedule_is_manual_and_catchup_disabled(dag) -> None:
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_quality_gates_do_not_retry(dag) -> None:
    assert dag.get_task("run_dbt_tests").retries == 0
    assert dag.get_task("validate_outputs").retries == 0
    assert dag.get_task("check_environment").retries == 0


def test_transient_tasks_retry_modestly(dag) -> None:
    assert dag.get_task("run_dbt_build").retries == 2
    assert dag.get_task("run_incrementality").retries == 2
    assert dag.get_task("check_database").retries == 2
