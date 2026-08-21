# Airflow DAG package

`retail_media_measurement_pipeline` orchestrates the frozen measurement path:

```text
check_environment → check_database → validate_sources
        → dbt run → dbt test
        → incrementality inference
        → experiment health + decisions
        → processed output validation
```

Airflow does not compute lift, health, or decisions. It runs existing scripts.

Local setup, trigger commands, and retry semantics: see **Automated Measurement Pipeline** in the repository README and `docs/airflow.md`.

Set `AIRFLOW__CORE__DAGS_FOLDER` to this directory (or to the repository `dags/` path). Do not name a repository package `airflow`; that would shadow the Apache Airflow import.
