# Retail Media Incrementality Platform

**A synthetic retail-media experimentation and analytics project for measuring whether advertising drives incremental customer behavior, rather than simply receiving credit for purchases that may have happened anyway.**

## Business Problem

Retail-media teams routinely report metrics such as impressions, clicks, attributed revenue, and ROAS. These metrics answer an important question:

> **Which campaign received credit for a purchase?**

But they do not answer a different and often more important business question:

> **Did the advertising actually cause additional purchases or revenue?**

A customer who buys after seeing an ad may have purchased even without the campaign. Relying only on attributed revenue can therefore overstate advertising effectiveness.

This project simulates how a retailer or retail-media network can use **randomized treatment and control groups** to estimate campaign incrementality and make more defensible budget decisions.

The core distinction is:

**Attribution:** How much revenue is credited to the campaign?

**Incrementality:** How much additional customer behavior occurred because shoppers were assigned to the advertising treatment?

---

## What the Project Does

The project creates a synthetic retail-media environment and implements the analytical workflow from campaign generation through commercial decision support.

```text
Synthetic shoppers, advertisers, and campaigns
                |
                v
Randomized treatment / control assignment
                |
                v
Simulated advertising and transactions
                |
                v
PostgreSQL raw data
                |
                v
Typed staging tables
                |
                v
Campaign KPI and experiment marts
                |
                v
Treatment-control lift estimates
                |
                v
Campaign rankings and budget recommendations
```

The system combines:

* **Python** for reproducible synthetic data generation, experimental assignment, database loading, and recommendations
* **PostgreSQL / SQL** for data transformation, campaign KPIs, experimental outcome construction, and analytical marts
* **Randomized A/B testing** for treatment-control incrementality measurement
* **Commercial analytics** for connecting campaign performance to budget recommendations
* **pytest** for validating data generation, experiment assignment, and analytical logic

---

## Experiment Design

### Experimental Unit

The experimental unit is a **member within a campaign**.

Eligible members are selected based on the campaign's retailer and target audience segment.

### Treatment

Members randomized to treatment are eligible to receive campaign advertising.

### Control

Members randomized to control are held out from campaign advertising.

The current simulation verifies that:

```text
Control-group ad events = 0
```

This prevents advertising contamination of the holdout group.

### Assignment

The current seed-0 simulation contains:

| Experiment metric           |  Value |
| --------------------------- | -----: |
| Campaigns                   |     24 |
| Campaign-member assignments | 60,357 |
| Treatment assignments       | 48,295 |
| Control assignments         | 12,062 |
| Treatment share             | 80.02% |
| Control share               | 19.98% |

The assignment procedure targets an approximately **80/20 treatment-control split** while maintaining roughly 500 control members per campaign.

---

## What Is Measured

The project contains two distinct measurement layers.

### 1. Campaign Performance

Standard retail-media metrics include:

* impressions
* clicks
* CTR
* spend
* CPC
* orders
* conversion rate
* attributed revenue
* ROAS
* average order value

These describe campaign delivery and attributed performance.

### 2. Experimental Incrementality

For each campaign, customer outcomes are compared between randomized treatment and control groups.

The primary campaign-level estimator is a difference in means:

$$
\hat{\tau}=

\bar{Y}_T - \bar{Y}_C
$$

where (Y) can represent conversion, orders per member, or revenue per member.

For conversion:

$$
\text{Absolute Lift}=

\hat{p}_T - \hat{p}_C
$$

where (\hat{p}_T) and (\hat{p}_C) are the treatment and control conversion rates.

Estimated incremental revenue is calculated as:

$$
\widehat{\text{Incremental Revenue}}=

n_T
\left(
\bar{Y}_T^{\text{revenue}}=

\bar{Y}_C^{\text{revenue}}
\right)
$$

where (n_T) is the number of treatment members and (\bar{Y}_T^{\text{revenue}}) and (\bar{Y}_C^{\text{revenue}}) are average revenue per member in the treatment and control groups.

This is an **ITT-style treatment-effect estimate** for the synthetic randomized experiment.

---

## Current Synthetic Results

All results below are generated from **synthetic data**. They demonstrate the behavior of the analytical system and should not be interpreted as real retailer or advertiser performance.

### Campaign Performance

| Metric                    | Current seed-0 result |
| ------------------------- | --------------------: |
| Synthetic members         |                25,000 |
| Advertisers               |                     8 |
| Campaigns                 |                    24 |
| Impressions               |             3,910,259 |
| Clicks                    |                23,993 |
| CTR                       |                0.614% |
| Ad spend                  |            $46,678.60 |
| Attributed orders         |                 1,360 |
| Attributed revenue        |            $57,964.34 |
| ROAS                      |                  1.24 |
| Average revenue per order |                $42.62 |

### Experiment Results

Across the current campaign experiments:

| Metric                               |                  Result |
| ------------------------------------ | ----------------------: |
| Overall treatment conversion rate    |                   5.84% |
| Overall control conversion rate      |                   3.03% |
| Overall treatment-control difference | +2.81 percentage points |
| Median campaign treatment CVR        |                   5.08% |
| Median campaign control CVR          |                   2.43% |
| Median campaign absolute lift        | +2.61 percentage points |
| Maximum campaign absolute lift       | +5.57 percentage points |
| Estimated incremental orders         |                  ~1,401 |
| Estimated incremental revenue        |                ~$58,802 |

The strongest current point estimate is Campaign 9:

```text
Treatment CVR:        9.51%
Control CVR:          3.94%
Absolute lift:       +5.57 percentage points
Incremental revenue: ~$4,626
```

These are **point estimates from a synthetic randomized experiment**. Statistical confidence intervals are not yet implemented.

---

## Attribution vs. Incrementality

The central analytical idea of the project is that attributed performance and incremental performance are not the same quantity.

The current synthetic run reports:

```text
Attributed revenue:                    $57,964
Experimentally estimated
incremental revenue:                   $58,802
```

These values happen to be similar in the current simulation, but they are produced by different measurement approaches.

**Attributed revenue** assigns campaign credit to transactions.

**Incremental revenue** estimates what additional revenue occurred relative to the randomized control group's counterfactual baseline.

The purpose of the project is not to demonstrate a particular dollar result. It is to demonstrate the analytical framework required to distinguish the two.

---

## Commercial Decision Layer

Campaign-level experiment results feed a deterministic decision layer that ranks campaigns using incremental performance and assigns an efficiency category.

The current recommendation mapping is:

| Efficiency flag      | Recommendation    |
| -------------------- | ----------------- |
| `high_impact`        | `increase_budget` |
| `moderate`           | `maintain`        |
| `low_impact`         | `monitor`         |
| inefficient campaign | `reduce_budget`   |

For the current seed-0 simulation:

| Recommendation  | Campaigns |
| --------------- | --------: |
| Increase budget |         2 |
| Maintain        |        10 |
| Monitor         |        12 |
| Reduce budget   |         0 |

The recommendation engine is intentionally **rule based**. It is not a machine-learning model.

Its purpose is to demonstrate how experimental results can be translated into an interpretable commercial action.

---

## Data Architecture

PostgreSQL is organized into three analytical layers.

### Raw

Synthetic CSV files are loaded into `raw.*` tables with minimal transformation.

### Staging

SQL converts raw values into typed analytical tables:

```text
staging.stg_members
staging.stg_campaigns
staging.stg_experiment_assignment
staging.stg_ad_events
staging.stg_transactions
```

### Marts

Business and experimentation logic is implemented in analytical marts including:

```text
marts.campaign_base_metrics
marts.campaign_funnel_metrics
marts.campaign_spend_metrics
marts.daily_campaign_trends
marts.executive_summary_metrics

marts.experiment_lift_metrics
marts.segment_performance_metrics

marts.campaign_incrementality_rankings
marts.campaign_efficiency_flags
```

The SQL layer demonstrates analytical patterns including:

* CTEs
* joins across campaign, customer, exposure, and transaction tables
* conditional aggregation
* `COUNT(*) FILTER`
* NULL-safe ratios
* funnel calculations
* treatment-control aggregation
* window functions and `RANK()`
* campaign-level KPI rollups

---

## Repository Structure

```text
retail-media-platform/
├── app/
│   └── core/
│       └── database.py
│
├── configs/
│   ├── experiment_config.yaml
│   ├── recommendation_config.yaml
│   └── simulation_config.yaml
│
├── data/
│   ├── synthetic/
│   │   ├── members.csv
│   │   ├── advertisers.csv
│   │   ├── campaigns.csv
│   │   ├── campaign_experiment_assignments.csv
│   │   ├── ad_events.csv
│   │   └── transactions.csv
│   │
│   └── processed/
│       ├── experiment_lift_metrics.csv
│       ├── segment_performance_metrics.csv
│       └── campaign_recommendations.csv
│
├── docs/
│   ├── attribution_methodology.md
│   ├── experiment_design.md
│   └── kpi_definitions.md
│
├── notebooks/
│   ├── 01_data_checks.ipynb
│   ├── 03_segment_lift_analysis.ipynb
│   └── 04_business_insights.ipynb
│
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
│
├── sql/
│   ├── staging/
│   └── marts/
│
├── tests/
├── .env.example
├── requirements.txt
└── README.md
```

---

## Tech Stack

### Python

* pandas
* NumPy
* SQLAlchemy
* psycopg2
* PyYAML / YAML configuration

### Data

* PostgreSQL
* SQL

### Testing

* pytest

### Analysis

* Jupyter notebooks

The current project does **not** contain a trained machine-learning model, production API, deployed dashboard, or MLflow experiment-tracking workflow.

---

## Running the Project

### 1. Create the Python Environment

```bash
python -m venv venv_rmp
source venv_rmp/bin/activate

pip install -r requirements.txt
```

Create `.env` from `.env.example` and configure the PostgreSQL connection.

---

### 2. Generate Synthetic Data

Run the generators in dependency order:

```bash
python scripts/generate_members.py
python scripts/generate_advertisers.py
python scripts/generate_campaigns.py
python scripts/assign_experiments.py
python scripts/generate_ad_events.py
python scripts/generate_transactions.py
```

Generated files are written to:

```text
data/synthetic/
```

The simulation seed is configured in:

```text
configs/simulation_config.yaml
```

---

### 3. Load Data into PostgreSQL

```bash
python scripts/load_to_postgres.py
```

This loads the generated CSV files into the PostgreSQL `raw` schema.

---

### 4. Build Staging Tables

Connect to PostgreSQL:

```bash
psql "$DATABASE_URL"
```

Then, from inside the `psql` shell, run the staging SQL files in dependency order:

```sql
\i sql/staging/stg_members.sql
\i sql/staging/stg_campaigns.sql
\i sql/staging/stg_experiment_assignment.sql
\i sql/staging/stg_ad_events.sql
\i sql/staging/stg_transactions.sql
```

These transformations convert the raw CSV-loaded tables into typed analytical tables under the `staging` schema.

---

### 5. Build Analytical Marts

While still inside the `psql` shell, run the mart SQL files in dependency order:

```sql
\i sql/marts/campaign_base_metrics.sql
\i sql/marts/campaign_funnel_metrics.sql
\i sql/marts/campaign_spend_metrics.sql
\i sql/marts/daily_campaign_trends.sql
\i sql/marts/executive_summary_metrics.sql

\i sql/marts/experiment_lift_metrics.sql
\i sql/marts/segment_performance_metrics.sql

\i sql/marts/campaign_incrementality_rankings.sql
\i sql/marts/campaign_efficiency_flags.sql
```

The first set builds standard campaign-performance metrics such as impressions, clicks, spend, CTR, and ROAS.

The experiment marts construct treatment and control outcomes and calculate campaign-level lift, incremental orders, and incremental revenue.

The final marts rank campaigns and assign efficiency categories used by the recommendation layer.

To leave PostgreSQL when finished:

```sql
\q
```

The incrementality marts can also be rebuilt and exported to CSV from Python with:

```bash
python scripts/run_incrementality.py --export-csv
```

---

### 6. Generate Campaign Recommendations

```bash
python scripts/generate_recommendations.py
```

Output:

```text
data/processed/campaign_recommendations.csv
```

---

### 7. Run Tests

```bash
python -m pytest -q
```

Current result:

```text
30 passed
```

---

## Validation

One important experiment-integrity check confirms that control members receive no campaign advertising:

```sql
SELECT COUNT(*) AS control_ad_events
FROM staging.stg_ad_events ae
JOIN staging.stg_experiment_assignment ea
  ON ae.campaign_id = ea.campaign_id
 AND ae.member_id = ea.member_id
WHERE ea.experiment_arm = 'control';
```

Expected result:

```text
0
```

Campaign lift results can be inspected with:

```sql
SELECT
    campaign_id,
    control_member_count,
    treatment_member_count,
    control_conversion_rate,
    treatment_conversion_rate,
    absolute_lift,
    incremental_orders,
    incremental_revenue
FROM marts.experiment_lift_metrics
ORDER BY incremental_revenue DESC;
```

---

## Testing

The project currently contains **30 passing tests** covering core simulation and analytical logic, including:

* generated data structure
* randomized experiment assignment
* treatment/control logic
* metric calculations
* incrementality calculations
* deterministic recommendation behavior

---

## Current Limitations

This project is intentionally focused on randomized retail-media measurement rather than broad platform development.

### No uncertainty estimates yet

Campaign lift and incremental revenue are currently point estimates. Standard errors, confidence intervals, or bootstrap intervals are the next statistical improvement.

### Synthetic data only

All customers, advertising activity, transactions, campaign results, and revenue are simulated. Reported dollar values demonstrate the analytical workflow and are not real business outcomes.

### No causal ML or uplift model

Segment-level analysis is descriptive subgroup analysis rather than CATE or individualized treatment-effect modeling.

### No observational causal inference

The primary causal design is randomized treatment/control assignment.

### No production application layer

The repository currently focuses on Python, PostgreSQL, experimentation, and analytical decision support rather than API or dashboard deployment.

---

## Next Improvements

The highest-value next additions are:

1. Add standard errors and 95% confidence intervals for campaign lift and incremental revenue.
2. Align segment-level outcome definitions with the primary campaign experiment.
3. Add a single reproducible command for rebuilding staging tables, marts, and analytical outputs.
4. Build a concise attribution-vs-incrementality comparison view for campaign decision making.
5. Add a lightweight dashboard only after the statistical layer is complete.

---

## Why This Project Matters

Retail-media measurement is not just a reporting problem.

A campaign can receive credit for customer purchases without causing those purchases. Reliable media decisions therefore require a counterfactual:

> **What would these customers have done without the advertising?**

This project demonstrates how randomized holdouts, SQL-based analytical pipelines, and treatment-effect estimation can turn that question into measurable campaign outcomes and interpretable business decisions.
