# KPI definitions

This document defines the core retail media metrics used in this platform. Definitions are written for business stakeholders; formulas use standard industry meaning so engineering and analytics can implement them consistently.

---

## Delivery and engagement

### Impressions

An **impression** is a single served ad view (or equivalent delivery event) counted when the ad is shown to a user or device in a measurable way. In practice, platforms apply **deduplication rules** (e.g., per day per user per placement) so totals match billing and reporting contracts.

**Typical use:** Reach and frequency planning, CPM-based spend, funnel volume upstream of clicks.

### Clicks

A **click** is a user-initiated engagement that the ad server records as navigation to the advertiser’s destination (product page, retailer site, etc.), subject to the platform’s **click validation** rules (e.g., filtering invalid or accidental clicks).

**Typical use:** Engagement quality, downstream conversion modeling, CPC-based spend.

### Click-through rate (CTR)

**CTR** is the share of impressions that resulted in at least one click:

\[
\text{CTR} = \frac{\text{Clicks}}{\text{Impressions}}
\]

Reported as a **percentage** or decimal depending on the dashboard. When aggregating over time or segments, use **ratio of sums** (total clicks ÷ total impressions), not an average of daily CTRs, unless you explicitly want a time-weighted or equal-weight average.

**Interpretation:** Creative and placement relevance; low CTR with high conversion can still be valuable for high-intent audiences.

### Conversion rate (CVR)

**CVR** (conversion rate) is the share of a defined **eligible population** that completes a **conversion event** within an agreed **attribution window**.

Common variants:

- **Click CVR:** \(\text{CVR}_{\text{click}} = \dfrac{\text{Conversions attributed to clicks}}{\text{Clicks}}\)
- **Impression CVR:** \(\text{CVR}_{\text{imp}} = \dfrac{\text{Conversions attributed to impressions}}{\text{Impressions}}\)

The **numerator** must use the same attribution rules as the rest of the report (e.g., last-touch within 7 days of click). The **denominator** must match the definition in your data contract (e.g., all clicks vs. clicks on in-stock items only).

**Interpretation:** Efficiency of traffic or of impression-to-outcome paths; compare across segments only when definitions align.

---

## Spend and outcomes

### Spend

**Spend** is **advertising cost** recognized in the period, in currency, according to the buying model:

- **CPM:** cost per thousand impressions  
- **CPC:** cost per click  
- **Fixed fee / sponsorship:** prorated or as-booked per contract  

Spend should reconcile to **invoice or platform billing** where possible. **Delivery-adjusted spend** may exclude non-billable impressions depending on policy.

### Attributed revenue

**Attributed revenue** is **order or sales revenue** assigned to marketing touchpoints using an **attribution model** (e.g., last-touch, first-touch, linear). It answers: *“How much revenue do we associate with this campaign or channel under our chosen credit-splitting rules?”*

It is **not** by itself a measure of **causal** impact: the same sale might have happened without the ad, or multiple channels might each claim partial credit.

### Return on ad spend (ROAS)

**ROAS** compares attributed revenue to spend:

\[
\text{ROAS} = \frac{\text{Attributed revenue}}{\text{Spend}}
\]

A ROAS of **4** means **\$4 of attributed revenue per \$1 spent** (units must be consistent). **MER** (marketing efficiency ratio) and ROAS are related concepts; this project standardizes on ROAS as above.

**Interpretation:** Useful for **budget pacing and mix** under a fixed attribution scheme; **not** interchangeable with incremental return unless attribution equals true incrementality (it usually does not).

---

## Incrementality and lift

### Lift

**Lift** is the **relative** improvement in an outcome metric for a **treated** group compared to a **control** group that was **not** (or less) exposed to the campaign, after alignment on time period and definitions:

\[
\text{Lift} = \frac{\bar{Y}_{\text{treatment}} - \bar{Y}_{\text{control}}}{\bar{Y}_{\text{control}}}
\]

where \(\bar{Y}\) is the mean outcome (e.g., conversion rate, orders per user, revenue per household) over the analysis window. Lift is often expressed as a **percentage** (e.g., 12% lift).

**Requirements:** Treatment and control must be defined by the **experiment or quasi-experiment** design; lift is **not** derived from attribution weights alone.

### Incremental revenue

**Incremental revenue** is the **additional revenue** estimated to be caused by the campaign, relative to what would have happened without the incremental media:

\[
\text{Incremental revenue} = \text{Revenue}_{\text{treatment}} - \text{Revenue}_{\text{control-like baseline}}
\]

In randomized holdouts, the baseline is observed **control** revenue scaled to the treatment population. In matched or model-based designs, the baseline is **counterfactual** (predicted or matched non-exposed behavior).

**Interpretation:** Answers *“How much extra revenue did we get because of this spend?”* under the assumptions of the measurement design.

### Incremental ROAS (iROAS)

**iROAS** (incremental return on ad spend) relates **incremental revenue** to **spend**:

\[
\text{iROAS} = \frac{\text{Incremental revenue}}{\text{Spend}}
\]

This is the **causal** analogue of ROAS when incremental revenue comes from a valid lift study. **iROAS** can be **lower than ROAS** when attribution **over-credits** the campaign, or **higher** when attribution **under-credits** (less common but possible depending on model and path complexity).

---

## How these KPIs work together

| Question | Primary KPIs |
|----------|----------------|
| Did the ad drive engagement? | Impressions, clicks, CTR |
| Did traffic convert under our attribution rules? | CVR, attributed revenue, ROAS |
| Did the ad **cause** more sales? | Lift, incremental revenue, iROAS |

For implementation details on experiments and attribution, see `experiment_design.md` and `attribution_methodology.md`.
