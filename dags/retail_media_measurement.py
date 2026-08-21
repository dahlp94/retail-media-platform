"""
Orchestrate the frozen retail-media measurement workflow.

Airflow coordinates existing dbt and Python scripts. It does not compute
treatment effects, experiment health, iROAS, or campaign decisions.

The underlying data is a frozen synthetic snapshot (seed 0). The DAG is
manual by default: schedule=None, catchup=False. A daily schedule would
only simulate recurring measurement, not live campaign arrival.

Canonical task order:

1. check_environment
2. check_database
3. validate_sources
4. run_dbt_build
5. run_dbt_tests
6. run_incrementality
7. run_experiment_decisions
8. validate_outputs
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow.sdk import DAG
except ImportError:  # Airflow 2.x
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

# dags/retail_media_measurement.py -> repository root
REPO_ROOT = Path(os.environ.get("RETAIL_MEDIA_REPO", Path(__file__).resolve().parents[1]))

DAG_ID = "retail_media_measurement_pipeline"

# Transient failures (connection, dbt run, Python scripts): modest retries.
TRANSIENT_RETRIES = 2
RETRY_DELAY = timedelta(seconds=30)

# Deterministic quality gates should fail the DAG, not hide behind retries.
QUALITY_RETRIES = 0

# Incrementality includes a 1,000-iteration member bootstrap per campaign.
INFERENCE_TIMEOUT = timedelta(hours=1)
DBT_TIMEOUT = timedelta(minutes=30)

default_args = {
    "owner": "retail-media",
    "depends_on_past": False,
    "retries": TRANSIENT_RETRIES,
    "retry_delay": RETRY_DELAY,
}


def _bash(command: str) -> str:
    """Run a repo-root command after loading .env. Do not echo secrets."""
    # Manual Airflow 3 runs with schedule=None have no logical date, so
    # bare {{ ds }} raises UndefinedError during Jinja render.
    return f"""
set -euo pipefail
cd "{REPO_ROOT}"
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
export PYTHONPATH="{REPO_ROOT}${{PYTHONPATH:+:$PYTHONPATH}}"
echo "dag_run_id={{{{ run_id }}}} logical_date={{{{ ds | default('none') }}}} task_id={{{{ task.task_id }}}}"
echo "command: {command}"
{command}
echo "status: success task_id={{{{ task.task_id }}}}"
""".strip()


with DAG(
    dag_id=DAG_ID,
    description=(
        "Orchestrate dbt transformations, warehouse tests, incrementality "
        "inference, experiment health, and campaign decisions on frozen "
        "synthetic retail-media data."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["measurement", "incrementality", "synthetic", "portfolio"],
) as dag:
    check_environment = BashOperator(
        task_id="check_environment",
        bash_command=_bash("python scripts/check_pipeline_readiness.py --skip-database"),
        retries=QUALITY_RETRIES,
        doc_md="Confirm POSTGRES_* / DATABASE_URL and required repository files.",
    )

    check_database = BashOperator(
        task_id="check_database",
        bash_command=_bash(
            "python scripts/check_pipeline_readiness.py --skip-sources"
        ),
        retries=TRANSIENT_RETRIES,
        retry_delay=RETRY_DELAY,
        doc_md="Confirm PostgreSQL accepts a connection.",
    )

    validate_sources = BashOperator(
        task_id="validate_sources",
        bash_command=_bash("python scripts/check_pipeline_readiness.py"),
        retries=1,
        retry_delay=RETRY_DELAY,
        doc_md=(
            "Confirm raw.* tables exist and are non-empty, and frozen "
            "synthetic CSVs are present. Does not regenerate data."
        ),
    )

    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=_bash("scripts/run_dbt.sh run"),
        retries=TRANSIENT_RETRIES,
        retry_delay=RETRY_DELAY,
        execution_timeout=DBT_TIMEOUT,
        doc_md="Rebuild staging, intermediate, and mart tables via dbt.",
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=_bash("scripts/run_dbt.sh test"),
        retries=QUALITY_RETRIES,
        execution_timeout=DBT_TIMEOUT,
        doc_md=(
            "Warehouse data tests. Failures are quality defects, not "
            "transient errors, so this task does not retry."
        ),
    )

    run_incrementality = BashOperator(
        task_id="run_incrementality",
        bash_command=_bash("python scripts/run_incrementality.py --export-csv"),
        retries=TRANSIENT_RETRIES,
        retry_delay=RETRY_DELAY,
        execution_timeout=INFERENCE_TIMEOUT,
        doc_md=(
            "Attach Wald conversion intervals and member-level bootstrap "
            "intervals to dbt point estimates. Overwrites marts and CSVs."
        ),
    )

    run_experiment_decisions = BashOperator(
        task_id="run_experiment_decisions",
        bash_command=_bash("python scripts/run_experiment_decisions.py --export-csv"),
        retries=TRANSIENT_RETRIES,
        retry_delay=RETRY_DELAY,
        execution_timeout=INFERENCE_TIMEOUT,
        doc_md=(
            "Attach SRM/health status, iROAS, and deterministic campaign "
            "decisions. Overwrites marts and CSVs."
        ),
    )

    validate_outputs = BashOperator(
        task_id="validate_outputs",
        bash_command=_bash("python scripts/validate_pipeline_outputs.py"),
        retries=QUALITY_RETRIES,
        doc_md=(
            "Publication check for processed design, health, and decision "
            "CSVs. Structural only; does not recompute statistics."
        ),
    )

    (
        check_environment
        >> check_database
        >> validate_sources
        >> run_dbt_build
        >> run_dbt_tests
        >> run_incrementality
        >> run_experiment_decisions
        >> validate_outputs
    )
