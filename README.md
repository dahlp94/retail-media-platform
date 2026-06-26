# Retail Media Measurement, Experimentation, and Incrementality Platform

**End-to-end retail media analytics that separates attributed performance from experimentally measured incremental impact — and turns both into clear budget guidance.**

## Problem statement

Retail media teams must decide whether ads *caused* incremental purchases, not just whether purchases happened after exposure. **Attribution** answers which campaign or touchpoint gets credit for a purchase. **Incrementality** answers a different question: what happened because of the campaign compared with what would have happened anyway.

Strong attributed ROAS can mask weak causal impact when baseline demand is high, when credit is assigned across touchpoints, or when treatment and control groups behave differently. Spend, creative, and audience decisions need experimental evidence, incremental orders, and incremental revenue — not attributed totals alone.

## Solution overview

This project simulates a production-style retail media analytics stack. Synthetic members, advertisers, campaigns, experiment assignments, ad events, and transactions feed a **PostgreSQL** pipeline organized into **raw → staging → marts** SQL layers.

The KPI layer reports standard campaign delivery and efficiency metrics, including impressions, clicks, CTR, CVR, spend, CPC, CPO, revenue, and ROAS. The experimentation layer uses treatment-control assignment to estimate **absolute lift**, **relative lift**, **incremental orders**, and **incremental revenue** at the campaign level. The decision layer ranks campaigns by incremental performance, classifies campaign efficiency, and generates budget-oriented recommendations.

The project is designed to show the difference between:

* **Attributed performance:** how much revenue is credited to a campaign.
* **Experimentally measured incrementality:** how much additional revenue the campaign caused.

## Key features

* **Synthetic data generation** — Reproducible members, advertisers, campaigns, experiment assignments, ad events, and transactions under configurable simulation assumptions in `configs/`.
* **PostgreSQL analytics pipeline** — Raw, staging, and mart schemas with SQL-first transformations.
* **Campaign KPI layer** — Campaign-level funnel metrics, spend metrics, daily trends, and executive summary rollups.
* **Incrementality analysis** — Treatment vs. control conversion rates, absolute lift, relative lift, incremental orders, and incremental revenue.
* **Segment-level performance** — Audience and geo-level lift views for deeper campaign diagnostics.
* **Decision layer** — Campaign rankings, efficiency flags, and stakeholder-facing budget recommendations.
* **Validation workflow** — Unit tests, notebooks, and SQL checks to confirm the simulation and marts produce reasonable outputs.

## Calibrated simulation results

The current calibrated run uses 24 campaigns and 25,000 synthetic members. The experiment design assigns roughly 500 control members and roughly 2,000 treatment members per campaign, giving stable treatment-control comparisons.

The current calibrated executive summary is:

| Metric                    |        Value |
| ------------------------- | -----------: |
| Total spend               | `$50,654.44` |
| Total revenue             | `$65,534.64` |
| Total orders              |      `1,579` |
| Total impressions         |  `4,032,753` |
| Total clicks              |     `22,579` |
| Active campaigns          |         `24` |
| Overall ROAS              |       `1.29` |
| Overall CTR               |      `0.56%` |
| Average revenue per order |     `$41.50` |

The experiment layer is calibrated so control groups show realistic baseline conversion, while treatment groups show moderate incremental lift on top of baseline demand.

| Experiment metric                |        Calibrated result |
| -------------------------------- | -----------------------: |
| Median control conversion rate   |                  `~2.8%` |
| Median treatment conversion rate |                  `~5.6%` |
| Median absolute lift             | `~2.8 percentage points` |
| Top campaign lift                | `~5–6 percentage points` |
| Zero-control-CVR campaigns       |                      `0` |

This avoids the common synthetic-data problem where control conversion is near zero and treatment lift appears artificially large.

## Key insights

* **Attribution and incrementality answer different questions.** ROAS shows attributed efficiency, while lift and incremental revenue show causal impact.
* **Baseline demand matters.** The simulation includes realistic control conversion, so measured lift is interpreted against a nonzero purchase baseline.
* **Not every campaign should be scaled.** Some campaigns generate strong incremental revenue, while others show modest lift and are better candidates for monitoring.
* **Top performers show meaningful but realistic lift.** In the calibrated run, the strongest campaigns produce roughly 4–6 percentage points of absolute conversion lift.
* **Budget decisions should anchor on incremental impact.** The decision layer uses lift and incremental revenue to generate recommendations rather than relying on attributed ROAS alone.

## Decision framework

Efficiency labels are derived from incrementality marts, especially absolute lift and incremental revenue. Each campaign receives an `efficiency_flag`, which maps deterministically to a budget recommendation.

| Efficiency signal                       | Recommendation    |
| --------------------------------------- | ----------------- |
| Strong incremental performance          | `increase_budget` |
| Middle-of-the-pack performance          | `maintain`        |
| Modest but positive lift                | `monitor`         |
| Poor lift or negative incremental value | `reduce_budget`   |

In the current calibrated run, the recommendation output is:

| Recommendation    | Campaign count |
| ----------------- | -------------: |
| `increase_budget` |            `6` |
| `maintain`        |            `7` |
| `monitor`         |           `11` |
| `reduce_budget`   |            `0` |

No campaign currently receives `reduce_budget` because all campaigns in the calibrated run show positive measured lift and positive incremental revenue. The logic still supports `reduce_budget` when a campaign meets the inefficient-spend threshold.

The script `scripts/generate_recommendations.py` joins campaign rankings and efficiency flags, then writes:

```text
data/processed/campaign_recommendations.csv
```

Example output:

```text
campaign_id,efficiency_flag,recommendation
4,high_impact,increase_budget
6,high_impact,increase_budget
9,high_impact,increase_budget
...
```

## Tech stack

* **Languages:** Python 3.11+, SQL
* **Database:** PostgreSQL
* **Python libraries:** pandas, NumPy, SciPy, scikit-learn, SQLAlchemy, psycopg2
* **Testing:** pytest
* **Config:** YAML, python-dotenv
* **Notebook analysis:** Jupyter notebooks
* **Future surfaces:** FastAPI, Uvicorn, Streamlit, MLflow

## Project structure

```text
retail-media-platform/
├── app/
│   └── core/
│       └── database.py
├── configs/
│   ├── experiment_config.yaml
│   ├── recommendation_config.yaml
│   └── simulation_config.yaml
├── data/
│   ├── raw/
│   ├── synthetic/
│   │   ├── advertisers.csv
│   │   ├── members.csv
│   │   ├── campaigns.csv
│   │   ├── campaign_experiment_assignments.csv
│   │   ├── ad_events.csv
│   │   └── transactions.csv
│   └── processed/
│       ├── experiment_lift_metrics.csv
│       ├── segment_performance_metrics.csv
│       └── campaign_recommendations.csv
├── docs/
│   ├── attribution_methodology.md
│   ├── experiment_design.md
│   └── kpi_definitions.md
├── notebooks/
│   ├── 01_data_checks.ipynb
│   ├── 03_segment_lift_analysis.ipynb
│   └── 04_business_insights.ipynb
├── scripts/
│   ├── generate_members.py
│   ├── generate_advertisers.py
│   ├── generate_campaigns.py
│   ├── assign_experiments.py
│   ├── generate_ad_events.py
│   ├── generate_transactions.py
│   ├── load_to_postgres.py
│   ├── run_incrementality.py
│   └── generate_recommendations.py
├── sql/
│   ├── staging/
│   │   ├── stg_members.sql
│   │   ├── stg_campaigns.sql
│   │   ├── stg_experiment_assignment.sql
│   │   ├── stg_ad_events.sql
│   │   └── stg_transactions.sql
│   └── marts/
│       ├── campaign_base_metrics.sql
│       ├── campaign_funnel_metrics.sql
│       ├── campaign_spend_metrics.sql
│       ├── daily_campaign_trends.sql
│       ├── executive_summary_metrics.sql
│       ├── experiment_lift_metrics.sql
│       ├── segment_performance_metrics.sql
│       ├── campaign_incrementality_rankings.sql
│       └── campaign_efficiency_flags.sql
├── tests/
│   ├── test_data_generation.py
│   ├── test_experiment_assignment.py
│   ├── test_incrementality.py
│   └── test_metrics.py
├── .env.example
├── requirements.txt
└── README.md
```

## How to run

### 1. Environment setup

Install PostgreSQL and Python 3.11+.

Create a virtual environment and install dependencies:

```bash
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and set your database connection variables. For example:

```bash
DATABASE_URL=postgresql://retail_media:password@localhost:5432/retail_media
```

### 2. Generate synthetic data

Run the scripts in order:

```bash
python scripts/generate_members.py
python scripts/generate_advertisers.py
python scripts/generate_campaigns.py
python scripts/assign_experiments.py
python scripts/generate_ad_events.py
python scripts/generate_transactions.py
```

This writes synthetic CSVs under:

```text
data/synthetic/
```

### 3. Load raw data into PostgreSQL

```bash
python scripts/load_to_postgres.py
```

This loads the generated CSVs into the `raw` schema.

### 4. Build staging tables

```bash
psql "$DATABASE_URL" -f sql/staging/stg_members.sql
psql "$DATABASE_URL" -f sql/staging/stg_campaigns.sql
psql "$DATABASE_URL" -f sql/staging/stg_experiment_assignment.sql
psql "$DATABASE_URL" -f sql/staging/stg_ad_events.sql
psql "$DATABASE_URL" -f sql/staging/stg_transactions.sql
```

### 5. Build mart tables

Run the marts in dependency order:

```bash
psql "$DATABASE_URL" -f sql/marts/campaign_base_metrics.sql
psql "$DATABASE_URL" -f sql/marts/campaign_funnel_metrics.sql
psql "$DATABASE_URL" -f sql/marts/campaign_spend_metrics.sql
psql "$DATABASE_URL" -f sql/marts/daily_campaign_trends.sql
psql "$DATABASE_URL" -f sql/marts/executive_summary_metrics.sql
psql "$DATABASE_URL" -f sql/marts/experiment_lift_metrics.sql
psql "$DATABASE_URL" -f sql/marts/segment_performance_metrics.sql
psql "$DATABASE_URL" -f sql/marts/campaign_incrementality_rankings.sql
psql "$DATABASE_URL" -f sql/marts/campaign_efficiency_flags.sql
```

As a shortcut for the incrementality marts, after upstream tables exist you can run:

```bash
python scripts/run_incrementality.py --export-csv
```

This executes the incrementality SQL and exports processed CSV snapshots.

### 6. Generate campaign recommendations

```bash
python scripts/generate_recommendations.py
```

This writes:

```text
data/processed/campaign_recommendations.csv
```

### 7. Run tests

```bash
python -m pytest -q
```

Expected result for the calibrated project:

```text
30 passed
```

## Useful validation queries

### Executive summary

```bash
psql "$DATABASE_URL" -c "SELECT * FROM marts.executive_summary_metrics;"
```

### Campaign lift table

```bash
psql "$DATABASE_URL" -c "
SELECT
    campaign_id,
    control_conversion_rate,
    treatment_conversion_rate,
    absolute_lift,
    incremental_orders,
    incremental_revenue
FROM marts.experiment_lift_metrics
ORDER BY incremental_revenue DESC;
"
```

### Recommendation counts

```bash
python - <<'PY'
import pandas as pd

df = pd.read_csv('data/processed/campaign_recommendations.csv')
print(pd.crosstab(df['efficiency_flag'], df['recommendation']))
print(df['recommendation'].value_counts())
PY
```

### Lift distribution summary

```bash
psql "$DATABASE_URL" -c "
SELECT
    COUNT(*) AS campaigns,
    MIN(control_member_count) AS min_control_n,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY control_member_count) AS median_control_n,
    MAX(control_member_count) AS max_control_n,
    MIN(treatment_member_count) AS min_treatment_n,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY treatment_member_count) AS median_treatment_n,
    MAX(treatment_member_count) AS max_treatment_n,
    MIN(control_conversion_rate) AS min_control_cvr,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY control_conversion_rate) AS median_control_cvr,
    MAX(control_conversion_rate) AS max_control_cvr,
    MIN(treatment_conversion_rate) AS min_treatment_cvr,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY treatment_conversion_rate) AS median_treatment_cvr,
    MAX(treatment_conversion_rate) AS max_treatment_cvr,
    MIN(absolute_lift) AS min_lift,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY absolute_lift) AS median_lift,
    MAX(absolute_lift) AS max_lift,
    COUNT(*) FILTER (WHERE control_conversion_rate = 0) AS zero_control_cvr_campaigns
FROM marts.experiment_lift_metrics;
"
```

## Notebooks

Explore the notebooks for validation and narrative analysis:

```text
notebooks/01_data_checks.ipynb
notebooks/03_segment_lift_analysis.ipynb
notebooks/04_business_insights.ipynb
```

Suggested notebook storyline:

1. Confirm synthetic data quality and table counts.
2. Compare campaign KPIs such as CTR, CVR, ROAS, and spend.
3. Analyze treatment-control lift by campaign.
4. Identify high-impact, moderate, and low-impact campaigns.
5. Translate incrementality results into budget recommendations.

## Testing and validation status

The calibrated project currently passes all tests:

```text
30 passed
```

The main validation checks are:

* Raw, staging, and mart tables build successfully.
* Campaign-level marts contain 24 campaigns.
* Treatment and control groups are large enough for stable lift estimates.
* Control conversion is nonzero and realistic.
* Incrementality metrics produce realistic lift values.
* Recommendation mapping is deterministic.

## Future improvements

* **Lift uncertainty** — Add confidence intervals, standard errors, or Bayesian credible intervals for absolute lift and incremental revenue.
* **Power analysis** — Add minimum detectable effect and sample-size diagnostics for each campaign.
* **Richer attribution** — Add last-touch, multi-touch, and experiment-calibrated attribution comparisons.
* **Over-attribution diagnostics** — Flag campaigns with strong ROAS but weak incremental lift.
* **Geo experiments** — Add geo holdouts, synthetic controls, and market-level treatment assignment.
* **Uplift modeling** — Estimate heterogeneous treatment effects by audience segment or customer features.
* **Workflow orchestration** — Add dbt, Airflow, or Makefile-based orchestration.
* **Product surfaces** — Add FastAPI endpoints and a Streamlit dashboard for campaign decisioning.
* **Experiment tracking** — Use MLflow to track simulation settings, calibration runs, and recommendation outputs.

## License

This project is released under the [MIT License](https://opensource.org/licenses/MIT).
