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
Treatment-control lift estimates + uncertainty
                |
                v
Experiment-health diagnostics
                |
                v
Attribution vs incrementality decision table
```

The system combines:

* **Python** for reproducible synthetic data generation, experimental assignment, database loading, and recommendations
* **PostgreSQL / SQL** for data transformation, campaign KPIs, experimental outcome construction, and analytical marts
* **Randomized A/B testing** for treatment-control incrementality measurement
* **Commercial analytics** for connecting campaign performance to budget recommendations
* **pytest** for validating data generation, experiment assignment, and analytical logic


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

These are **point estimates from a synthetic randomized experiment**, now reported with statistical uncertainty (analytic 95% CIs for conversion lift; member-level bootstrap 95% CIs for incremental orders and revenue). They are not live advertiser results.


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

The Stage 4 decision table makes the same distinction with two return metrics:

```text
ROAS  = attributed revenue / spend
iROAS = estimated incremental revenue / spend
```

Attributed ROAS answers how much credited revenue was associated with a dollar of spend. iROAS answers how much experimentally incremental revenue was estimated per dollar of spend. They can disagree.


## Experiment Health

Before a treatment-effect estimate is used for a budget decision, the project checks whether the randomized experiment is structurally trustworthy.

Campaign-level diagnostics include:

* randomized-arm counts versus the campaign's stored holdout fraction
* sample-ratio mismatch (chi-square goodness-of-fit)
* control-group advertising leakage
* duplicate `campaign_id × member_id` assignments
* assignment-to-outcome completeness
* pre-treatment standardized differences for signup tenure and pre-campaign conversion

Overall status is one of `PASS`, `WARN`, or `FAIL`.

`FAIL` is reserved for structural problems that make causal interpretation unsafe: control exposure leakage, missing assigned members in the outcome table, duplicate randomized assignments, or severe SRM (`p < 0.001`). Mild SRM (`0.001 ≤ p < 0.05`) or a pre-treatment standardized difference above 0.10 produces `WARN` and does not automatically invalidate the experiment.

Planned power and planned MDE are **not** reconstructed. The simulator never stored a pre-registered design calculation, so those metadata fields are left null. After the experiment, precision is summarized with confidence-interval width rather than post-hoc observed power.

For the current seed-0 run:

```text
PASS: 22 campaigns
WARN:  2 campaigns
FAIL:  0 campaigns
Control impressions on holdout members: 0
Missing assigned outcomes: 0
```


## Commercial Decision Layer

There are two deterministic outputs.

### 1. Simple efficiency mapping (legacy)

`data/processed/campaign_recommendations.csv` maps incrementality efficiency flags to a coarse action. It does **not** use experiment health or interval estimates.

| Efficiency flag      | Recommendation    |
| -------------------- | ----------------- |
| `high_impact`        | `increase_budget` |
| `moderate`           | `maintain`        |
| `low_impact`         | `monitor`         |
| inefficient campaign | `reduce_budget`   |

Current seed-0 mapping: 2 increase / 10 maintain / 12 monitor / 0 reduce.

### 2. Health-aware measurement decisions

`data/processed/campaign_measurement_decisions.csv` is the Stage 4 decision table. It combines attributed ROAS, incremental revenue and iROAS with uncertainty, experiment health, and efficiency flags.

Decision order is:

1. If experiment health is `FAIL`, assign `do_not_interpret`.
2. Otherwise use the incremental-revenue 95% CI, not a p-value cutoff.
3. If that CI is entirely positive and iROAS ≥ 1, assign `increase_budget`.
4. If that CI is entirely positive but iROAS < 1, assign `maintain`.
5. If that CI is entirely negative, assign `reduce_budget`.
6. If the CI includes zero, assign `inconclusive`.

A significant-looking point estimate is not enough. A wide interval that includes zero is treated as inconclusive. A failed experiment is not interpreted even if the point estimate is large and positive.

Current seed-0 health-aware decisions:

| Decision          | Campaigns |
| ----------------- | --------: |
| Increase budget   |        14 |
| Maintain          |         3 |
| Inconclusive      |         7 |
| Reduce budget     |         0 |
| Do not interpret  |         0 |

Both layers are **rule based**. Neither is a trained recommendation model.


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
marts.experiment_member_outcomes
marts.segment_performance_metrics
marts.experiment_design_metadata
marts.experiment_health_metrics
marts.campaign_measurement_decisions

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


## Repository Structure

```text
retail-media-platform/
├── app/
│   ├── core/
│   │   └── database.py
│   └── statistics/
│       ├── experiment_inference.py
│       ├── experiment_health.py
│       └── experiment_decisions.py
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
│       ├── experiment_design_metadata.csv
│       ├── experiment_health_metrics.csv
│       ├── campaign_measurement_decisions.csv
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
│   ├── 04_business_insights.ipynb
│   └── 05_experiment_health_decisions.ipynb
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
│   ├── run_experiment_decisions.py
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


## Running the Project

### 1. Create the Python Environment

```bash
python -m venv venv_rmp
source venv_rmp/bin/activate

pip install -r requirements.txt
```

Create `.env` from `.env.example` and configure the PostgreSQL connection.


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


### 3. Load Data into PostgreSQL

```bash
python scripts/load_to_postgres.py
```

This loads the generated CSV files into the PostgreSQL `raw` schema.


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


### 5. Build Analytical Marts

While still inside the `psql` shell, run the mart SQL files in dependency order:

```sql
\i sql/marts/campaign_base_metrics.sql
\i sql/marts/campaign_funnel_metrics.sql
\i sql/marts/campaign_spend_metrics.sql
\i sql/marts/daily_campaign_trends.sql
\i sql/marts/executive_summary_metrics.sql

\i sql/marts/experiment_lift_metrics.sql
\i sql/marts/experiment_member_outcomes.sql
\i sql/marts/segment_performance_metrics.sql
\i sql/marts/experiment_design_metadata.sql
\i sql/marts/experiment_health_metrics.sql

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

The incrementality marts can also be rebuilt from Python. This step attaches conversion-lift standard errors and member-level bootstrap intervals to `marts.experiment_lift_metrics`, then optionally exports CSV:

```bash
python scripts/run_incrementality.py --export-csv
```

Then build experiment-health diagnostics, iROAS, and the health-aware decision table:

```bash
python scripts/run_experiment_decisions.py --export-csv
```

This does not regenerate synthetic source data and does not recompute the frozen v1 treatment-effect point estimates.


### 6. Generate Campaign Recommendations

The simple efficiency-flag mapping is unchanged:

```bash
python scripts/generate_recommendations.py
```

Output:

```text
data/processed/campaign_recommendations.csv
```

The health-aware decision table is written separately by `run_experiment_decisions.py`:

```text
data/processed/campaign_measurement_decisions.csv
```


### 7. Run Tests

```bash
python -m pytest -q
```

Current result:

```text
102 passed
```


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


## Testing

The project tests core simulation and analytical logic, including:

* generated data structure
* randomized experiment assignment
* treatment/control logic
* metric calculations
* incrementality calculations
* conversion-lift standard errors, confidence intervals, and p-values
* member-level bootstrap behavior for orders and revenue
* sample-ratio mismatch, control leakage, and outcome-completeness diagnostics
* iROAS transformation from incremental revenue and spend
* health-aware deterministic decisions, including withholding interpretation after a failed experiment


## Current Limitations

This project is intentionally focused on randomized retail-media measurement rather than broad platform development.

### Wald intervals for conversion can be degenerate

When an arm has conversion rate 0 or 1, the binomial variance estimate is zero. The implementation returns a collapsed interval rather than dividing by zero; that is a limitation of the Wald SE, not infinite precision.

### Bootstrap is percentile, not BCa

Orders and revenue use a stratified member-level percentile bootstrap. Bias-corrected intervals are not implemented.

### Synthetic data only

All customers, advertising activity, transactions, campaign results, and revenue are simulated. Reported dollar values demonstrate the analytical workflow and are not real business outcomes.

### Baseline balance is diagnostic, not a validity veto

Within a campaign, eligibility is retailer plus target audience segment, so segment does not vary. Geography is sparse at the control-arm cell size. The implemented checks are standardized differences on signup tenure and pre-campaign conversion. A large standardized difference produces `WARN` only.

### No causal ML or uplift model

Segment-level analysis is a descriptive subgroup analysis of the same campaign-window RCT outcomes as the primary experiment, not CATE or individualized treatment-effect modeling.

### No observational causal inference

The primary causal design is randomized treatment/control assignment.

### No production application layer

The repository currently focuses on Python, PostgreSQL, experimentation, and analytical decision support rather than API or dashboard deployment.

### Planned power and MDE are not reconstructed

The simulator did not store a pre-registered minimum detectable effect or power calculation. Those metadata fields are null by design. Interval width is used as the post-experiment precision summary.


## Next Improvements

The highest-value next additions are:

1. Add a single reproducible command for rebuilding staging tables, marts, and analytical outputs.
2. Add a lightweight dashboard only after the measurement and decision layer is complete.


## Why This Project Matters

Retail-media measurement is not just a reporting problem.

A campaign can receive credit for customer purchases without causing those purchases. Reliable media decisions therefore require a counterfactual:

> **What would these customers have done without the advertising?**

This project demonstrates how randomized holdouts, SQL-based analytical pipelines, treatment-effect estimation, experiment-integrity checks, and deterministic decision rules can turn that question into measurable campaign outcomes and interpretable business decisions.
