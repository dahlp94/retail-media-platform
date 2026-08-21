# Local Airflow for the measurement pipeline

This is a **local portfolio orchestration setup**, not a production Airflow deployment. The DAG runs against frozen synthetic data (seed 0). Do not treat a daily schedule as live campaign ingestion.

Stage 6 uses **Airflow 3.x**. Airflow 2.x cannot share this analytics virtualenv because it requires SQLAlchemy 1.4, while pandas and dbt here use SQLAlchemy 2.

## Dependencies

Use the existing analytics virtualenv, then add Airflow as an extra:

```bash
python -m venv venv_rmp
source venv_rmp/bin/activate
pip install -r requirements.txt
pip install -r requirements-airflow.txt
```

Airflow stays out of `requirements.txt` so a full Airflow constraints file cannot pin pandas or pydantic out from under dbt and the incrementality tests.

If `libcst` tries to compile from source on macOS, install a wheel first:

```bash
pip install 'libcst>=1.8.2' --only-binary=:all:
pip install -r requirements-airflow.txt
```

## Environment

Copy `.env.example` to `.env` and set PostgreSQL credentials. Then:

```bash
export RETAIL_MEDIA_REPO="$(pwd)"
export AIRFLOW_HOME="$RETAIL_MEDIA_REPO/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$RETAIL_MEDIA_REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
export PYTHONPATH="$RETAIL_MEDIA_REPO${PYTHONPATH:+:$PYTHONPATH}"
```

`AIRFLOW_HOME` must **not** be the repository `dags/` folder. Metadata, logs, and the metadata DB belong under `.airflow/` (gitignored).

Load `.env` before starting Airflow so `POSTGRES_*` are available to the scheduler, or rely on each task sourcing `.env` from the repo root (the DAG does this).

## Initialize and start

Preferred local command (creates metadata, a simple admin user, and all processes):

```bash
airflow db migrate
airflow standalone
```

Equivalent split processes if you want them in separate terminals:

```bash
airflow api-server --port 8080
airflow scheduler
```

Use the same venv, `AIRFLOW_HOME`, and `AIRFLOW__CORE__DAGS_FOLDER` in every terminal.

## Locate and trigger the DAG

UI: http://localhost:8080 → `retail_media_measurement_pipeline`

CLI:

```bash
airflow dags list
airflow dags unpause retail_media_measurement_pipeline
airflow dags trigger retail_media_measurement_pipeline
```

Inspect task logs in the UI (Grid → task → Log) or under `$AIRFLOW_HOME/logs`.

```bash
airflow tasks list retail_media_measurement_pipeline
```

Rerun a failed task from the UI by clearing the task instance. After a `run_dbt_tests` failure, do not clear later Python tasks until dbt tests pass.

## Manual equivalent (no Airflow)

```bash
scripts/run_analytics.sh
```

or the same sequence the DAG calls:

```bash
python scripts/check_pipeline_readiness.py
scripts/run_dbt.sh run
scripts/run_dbt.sh test
python scripts/run_incrementality.py --export-csv
python scripts/run_experiment_decisions.py --export-csv
python scripts/validate_pipeline_outputs.py
```

These scripts remain the debugging and CI path. The DAG only orchestrates them.

## Schedule

`schedule=None` and `catchup=False`. The dataset does not grow daily. Enabling `0 6 * * *` would only demonstrate a recurring job against the same frozen snapshot.
