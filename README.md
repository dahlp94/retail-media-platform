# Retail Media Measurement, Experimentation, and Incrementality Platform

**End-to-end retail media analytics that separates attributed performance from experimentally measured incremental impact—and turns both into clear budget guidance.**


## Problem statement

Retail media teams must decide whether ads *caused* incremental purchases, not just whether purchases occurred after exposure. **Attribution** (rules or models that assign credit along the path to purchase) answers a different question than **incrementality** (what happened because of the campaign versus what would have happened anyway).

Strong attributed ROAS can mask weak causal lift when baseline demand is high, when credit is shared across touchpoints, or when treatment and control behave differently. Decisions about spend, creative, and audience strategy need **experimental or quasi-experimental** evidence and **incremental orders and revenue**, not attributed totals alone.


## Solution overview

This project simulates a production-style stack similar in spirit to large retail media programs (e.g., Walmart MAP, Amazon Ads): synthetic members, campaigns, ad events, and transactions feed a **PostgreSQL** pipeline with **raw → staging → marts** SQL layers.

The KPI layer reports standard delivery and efficiency metrics (CTR, CVR, CPC, CPO, ROAS). The **experimentation layer** uses treatment vs. control assignment to estimate **absolute and relative lift**, plus **incremental orders and revenue** at the campaign level. A **decision layer** ranks campaigns on incremental performance, classifies efficiency, and outputs **budget-oriented recommendations** alongside notebooks for validation and storytelling.


## Key features

- **Synthetic data generation** — Reproducible members, advertisers, campaigns, experiment assignments, ad events (impressions, clicks), and transactions under configurable simulation settings (`configs/`).
- **KPI layer** — Campaign and platform metrics: funnel KPIs, spend and ROAS, daily trends, executive summary rollups (`sql/marts/`).
- **Incrementality analysis** — Treatment vs. control conversion rates, lift, and scaled incremental orders and revenue; segment-level performance for audience insights (`sql/marts/experiment_lift_metrics.sql`, `sql/marts/segment_performance_metrics.sql`).
- **Decision layer** — Campaign ranking from incremental signals, efficiency classification (e.g., high impact vs. inefficient spend), and CSV recommendations for stakeholders (`sql/marts/campaign_incrementality_rankings.sql`, `sql/marts/campaign_efficiency_flags.sql`, `scripts/generate_recommendations.py`).


## Key insights

The simulation is calibrated so **control groups show realistic baseline conversion** (on the order of **~3%**), with **treatment uplift** layered on top—so “lift” is interpretable against real-world baseline demand rather than artificial zero-control conversion.

- **Not every campaign is incremental.** Some campaigns show strong relative and absolute lift; others show minimal or negative lift relative to control. Portfolio-level averages hide winners and laggards.
- **High attributed ROAS does not guarantee high causal lift.** Campaigns can look efficient on attributed revenue while experiment-based lift is flat or negative—classic **over-attribution** risk when optimizing on rules-based credit alone.
- **Top performers can drive large incremental conversion lift.** In this simulation, standouts often land in the **roughly 20–30 percentage-point** range on absolute lift (treatment minus control conversion rate), making them clear candidates for **more budget** when the goal is incremental outcomes.
- **Inefficient spend is visible in the experiment layer.** Campaigns with weak lift despite meaningful spend surface as **candidates to reduce or restructure**, not to scale on attributed metrics alone.
- **Decisions should anchor on incremental impact.** Rankings and recommendations combine incremental revenue and lift signals so **budget moves align with causal performance**, not only last-touch or rule-based attribution.


## Decision framework

Efficiency labels are derived from **incrementality marts** (lift, incremental revenue, and related signals), not from attributed ROAS in isolation. Each campaign receives an **efficiency flag** (e.g., high impact, moderate, low impact, inefficient). Those flags map deterministically to **budget recommendations**:

| Efficiency signal | Recommendation (output) |
|-------------------|-------------------------|
| Strong incremental performance | **Increase budget** (`increase_budget`) |
| Middle-of-the-pack | **Maintain** (`maintain`) |
| Modest lift relative to peers | **Monitor** (`monitor`) |
| Poor lift vs. spend / opportunity | **Reduce budget** (`reduce_budget`) |

The script `scripts/generate_recommendations.py` joins rankings and flags and writes `data/processed/campaign_recommendations.csv` for reporting and downstream use.


## Tech stack

- **Languages:** Python 3.11+, SQL  
- **Database:** PostgreSQL (raw, staging, and marts schemas)  
- **Python libraries:** pandas, NumPy, SciPy, scikit-learn; SQLAlchemy and psycopg2 for database access  
- **API / UI (dependencies available):** FastAPI, Uvicorn, Streamlit  
- **Config & tooling:** python-dotenv, pytest, MLflow (for future experiment tracking)  


## Project structure

```text
retail-media-platform/
├── app/
│   └── core/              # Database connectivity (e.g., database.py)
├── configs/               # Simulation and experiment YAML configs
├── data/
│   ├── raw/               # Placeholder for raw layout; CSVs generated under synthetic/
│   ├── synthetic/         # Generated CSVs (members, campaigns, events, transactions, …)
│   └── processed/         # Exported mart snapshots (e.g., lift metrics, recommendations)
├── docs/                  # KPI definitions, experiment design, attribution notes
├── notebooks/             # Data checks, segment lift, business insights
├── scripts/               # Data generation, load, incrementality run, recommendations
├── sql/
│   ├── staging/           # Typed views/tables from raw
│   └── marts/             # KPI, incrementality, and decision-support tables
├── tests/                 # Unit tests for metrics and incrementality logic
├── .env.example
├── requirements.txt
└── README.md
```


## How to run

**1. Environment**

- Install **PostgreSQL** and Python **3.11+**.
- Copy `.env.example` to `.env` and set `DATABASE_URL` or `POSTGRES_*` variables so `app.core.database` can connect.
- Create a virtual environment and install dependencies:

```bash
pip install -r requirements.txt
```

**2. Generate synthetic data** (order matters; default output is `data/synthetic/`)

```bash
python scripts/generate_members.py
python scripts/generate_advertisers.py
python scripts/generate_campaigns.py
python scripts/assign_experiments.py
python scripts/generate_ad_events.py
python scripts/generate_transactions.py
```

**3. Load into PostgreSQL (raw layer)**

```bash
python scripts/load_to_postgres.py
```

**4. Build staging and marts**

Execute the SQL in `sql/staging/` (all `stg_*.sql` files), then apply `sql/marts/` in an order that respects dependencies—for example:

`campaign_base_metrics.sql` → `campaign_funnel_metrics.sql` → `campaign_spend_metrics.sql` → `daily_campaign_trends.sql` → `executive_summary_metrics.sql` → `experiment_lift_metrics.sql` → `segment_performance_metrics.sql` → `campaign_incrementality_rankings.sql` → `campaign_efficiency_flags.sql`

You can run these with `psql`, your SQL client, or any orchestration you prefer. As a shortcut for the two incrementality table scripts, after upstream marts exist you can run:

```bash
python scripts/run_incrementality.py --export-csv
```

That executes `experiment_lift_metrics.sql` and `segment_performance_metrics.sql` and optionally exports `data/processed/experiment_lift_metrics.csv` and `segment_performance_metrics.csv`. Apply the remaining mart files (including decision-layer SQL) as needed so `marts.campaign_incrementality_rankings` and `marts.campaign_efficiency_flags` exist.

**5. Generate recommendations**

```bash
python scripts/generate_recommendations.py
```

Writes `data/processed/campaign_recommendations.csv`.

**6. Tests and notebooks**

```bash
pytest
```

Explore `notebooks/01_data_checks.ipynb`, `notebooks/03_segment_lift_analysis.ipynb`, and `notebooks/04_business_insights.ipynb` for validation and narrative analysis.


## Future improvements

- **Richer attribution** — Multi-touch models and calibration against experimental benchmarks.  
- **Real-time or near-real-time pipelines** — Streaming events, incremental dbt/Airflow-style orchestration, and SLA-oriented marts.  
- **Advanced causal inference** — Geo tests, synthetic controls, and uplift modeling behind the same SQL-first interfaces.  
- **Product surfaces** — Production FastAPI endpoints and Streamlit or BI dashboards on top of existing marts.  
