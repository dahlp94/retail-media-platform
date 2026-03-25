# Experiment design for incrementality

This document describes how **treatment** and **control** groups support **incremental** measurement in retail media: holdouts, timing, and how **lift** is calculated. It is written for business owners of tests and for analysts implementing them.

---

## Why experiments matter

**Attribution** assigns credit to touchpoints along observed paths; it does not by itself prove that the ad **caused** the outcome. **Experiments** (or rigorous quasi-experiments) compare outcomes between groups that differ **only** in exposure to the campaign, so measured differences support **causal** statements about incrementality within stated assumptions.

---

## Treatment and control

### Treatment group

The **treatment** group consists of users, households, stores, or geographies **eligible to receive** the campaign (or the specific creative / tactic under test). Outcomes (orders, revenue, visits) are measured **after** exposure starts, using the same **conversion definitions** as operational reporting where possible.

### Control group

The **control** group consists of comparable units that are **held out** from the campaign (or receive a **placebo** / different spend level), so their outcomes approximate **what would have happened without** the incremental media.

**Key principle:** Treatment and control should be **similar before** the test on observable drivers of the outcome (e.g., historical purchase rate, seasonality), and should experience the **same external shocks** (promotions, holidays) except for the media being tested.

### Assignment mechanisms (conceptual)

| Approach | Business summary |
|----------|-------------------|
| **Randomized holdout** | Platform or retailer randomly assigns eligible users/geos to “see ads” vs “do not see ads” (or reduced frequency). Strongest for causal claims when execution is clean. |
| **Geo holdout** | Selected regions receive no (or less) media; others are treated. Good when user-level randomization is infeasible; requires care for cross-border shopping and spillover. |
| **Audience split** | Matched or randomly split audience lists. Requires stable identity and minimal leakage between arms. |

This platform assumes designs that yield a clear **binary or multi-cell** treatment indicator per analysis unit and period; advanced designs (switchback, synthetic control) can follow the same **lift** logic with appropriate baseline modeling.

---

## Holdouts

A **holdout** is the subset of the eligible population **intentionally not exposed** (or exposed to a controlled alternative) so it can serve as the **counterfactual benchmark**.

**Practices:**

- **Size:** Large enough for stable outcome rates; small holdouts increase variance on lift estimates.
- **Duration:** Aligned to **sales cycles** and **adstock** (carryover); ending a test too early biases lift.
- **Ethics and fairness:** Holdouts should respect partner policies and not withhold critical information where policy requires parity.
- **Leakage:** If holdout users see the same message through other channels, measured lift is **diluted** (underestimated incremental impact).

---

## Analysis windows

An **analysis window** is the time range over which outcomes are **aggregated and compared** between treatment and control.

### Pre-period (optional)

Used to **validate balance**: treatment and control should show similar trends or levels **before** the campaign. Large pre-period differences suggest confounding or assignment bias.

### Test period

Starts when **treatment exposure** is live and stable; ends when the test stops or when **lagged effects** are no longer material, per policy (e.g., 7- or 14-day conversion window after last impression).

### Cool-down

Some teams extend the window slightly beyond last spend to capture **delayed conversions**; the window must match the **attribution window** used for operational CVR/ROAS if you want to reconcile **directionally** (exact reconciliation is still model-dependent).

**Rule of thumb:** Define windows **up front** in the test plan to avoid **peeking** bias; if interim reads are needed, use pre-specified checkpoints or statistical adjustments.

---

## How lift is calculated

### Outcome metric

Choose a **primary outcome** \(Y\) per analysis unit (e.g., user, household, store-week):

- Binary: purchased yes/no in window  
- Count: orders in window  
- Continuous: revenue in window  

Aggregate to **arm-level** means:

- \(\bar{Y}_T\) = mean outcome in **treatment**  
- \(\bar{Y}_C\) = mean outcome in **control**  

### Relative lift

\[
\text{Lift} = \frac{\bar{Y}_T - \bar{Y}_C}{\bar{Y}_C}
\]

If \(\bar{Y}_C = 0\), lift is undefined; use absolute difference or an alternative parameterization.

### Absolute incremental effect (per unit)

\[
\Delta = \bar{Y}_T - \bar{Y}_C
\]

### Scaling to incremental revenue (illustrative)

Let \(N_T\) be the number of **treated** units (e.g., exposed households). If \(Y\) is **revenue per unit** in the window:

\[
\text{Incremental revenue} \approx N_T \times (\bar{Y}_T - \bar{Y}_C)
\]

When treatment and control **sizes differ** or weights apply, use **population-weighted** formulas or regression with appropriate weights so totals reflect the business.

### Uncertainty

In practice, report **confidence intervals** or **credibility intervals** for lift and incremental revenue (e.g., from standard errors, bootstrap, or Bayesian models). Narrow holdouts and noisy outcomes widen intervals.

### iROAS linkage

With **incremental revenue** and **treatment-side spend** (or incremental spend vs. control):

\[
\text{iROAS} = \frac{\text{Incremental revenue}}{\text{Spend (attributed to the test)}}
\]

Spend in the denominator should match **what incremental budget** the test is evaluating (e.g., incremental CPM/CPC in treatment geos only).

---

## Summary

| Concept | Role |
|---------|------|
| Treatment / control | Defines who receives the campaign vs. the benchmark |
| Holdout | Operational way to form a clean control |
| Analysis window | Fixes when outcomes count; reduces ad hoc slicing |
| Lift | Relative causal signal under design assumptions |
| Incremental revenue / iROAS | Dollar scaling of that signal for budgeting |

For metric definitions that align with reporting (CTR, ROAS, etc.), see `kpi_definitions.md`. For how this differs from rule-based credit assignment, see `attribution_methodology.md`.
