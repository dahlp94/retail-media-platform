# dbt trusted analytics layer

This folder is the **canonical SQL transformation project** for the retail-media measurement path.

It does **not** replace:

- synthetic data generation
- Wald / bootstrap inference
- SRM p-values and baseline SMDs
- Stage 4 campaign decisions
- iROAS (Python transformation of incremental-revenue intervals)

Those remain in Python. dbt owns warehouse typing, joins, grains, point-estimate marts, documentation, and data tests.

This is a synthetic portfolio warehouse, not a production data platform.


## Layout

```text
dbt/
├── dbt_project.yml
├── profiles.yml          # env-driven; no secrets
├── macros/
├── models/
│   ├── sources.yml       # raw.* CSV loads
│   ├── staging/          # typed cleanup
│   ├── intermediate/     # assigned campaign-member population
│   └── marts/
│       ├── incrementality/
│       ├── attribution/
│       └── commercial/
└── tests/                # experiment-integrity assertions
```

Warehouse schemas written by dbt:

```text
staging
intermediate
marts
```

`generate_schema_name` uses those schema names as-is so Python can keep reading `staging.*` and `marts.*`.


## Two lineages

**Incrementality** (causal ITT inputs):

```text
raw.campaign_experiment_assignments
        ↓
stg_experiment_assignment
        ↓
int_experiment_assigned_population
        ↓
experiment_member_outcomes     ← all in-window purchases; zeros kept
        ↓
experiment_lift_metrics        ← point estimates only
segment_performance_metrics
experiment_health_metrics      ← integrity inputs only
        ↓
Python inference + decisions
```

**Attribution** (credit assignment):

```text
stg_ad_events + stg_transactions.source_campaign_id
        ↓
campaign_base_metrics
        ↓
campaign_spend_metrics         ← attributed ROAS
```

Randomized outcomes must not use `source_campaign_id`.


## Connection

`profiles.yml` reads `POSTGRES_*` environment variables from the repository `.env`. It does not store passwords.

```bash
# from repo root
cp .env.example .env   # if needed
scripts/run_dbt.sh debug
scripts/run_dbt.sh run
scripts/run_dbt.sh test
```

Equivalent without the wrapper (after `source .env`):

```bash
dbt debug --project-dir dbt --profiles-dir dbt
dbt run   --project-dir dbt --profiles-dir dbt
dbt test  --project-dir dbt --profiles-dir dbt
```


## Docs / lineage

```bash
scripts/run_dbt.sh docs generate
scripts/run_dbt.sh docs serve
```

dbt 1.8 / 1.9 is pinned in `requirements.txt`. dbt-core 1.12 failed to install in this environment because of an experimental parser wheel, so Stage 5 stays on 1.9.

Do not commit `target/` artifacts.


## After dbt run

Point-estimate marts are complete. Then:

```bash
python scripts/run_incrementality.py --export-csv
python scripts/run_experiment_decisions.py --export-csv
```

`run_incrementality.py` attaches conversion CIs and bootstrap intervals.
`run_experiment_decisions.py` attaches SRM/health status, iROAS, and decisions.

Re-running `dbt run` rebuilds SQL tables and **drops** those Python enrichment columns. Always run Python after dbt.


## Legacy SQL

The files under `sql/staging/` and `sql/marts/` are frozen references for the pre-dbt implementation. They are not the default build path.

`--legacy-sql` on the Python scripts can still execute those files in an emergency. Prefer `dbt run`.
