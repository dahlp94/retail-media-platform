# Attribution methodology

This document explains **multi-touch attribution** models used for **reported** credit assignment—**first-touch**, **last-touch**, and **linear**—and clarifies why **attribution** is not the same as **incrementality**. It is intended for business users who interpret dashboards and for teams aligning SQL and product logic.

---

## What attribution answers

**Attribution** answers: *“Given that a conversion happened, how should we **split credit** across the touchpoints we observed on the path?”*

It is a **bookkeeping and fairness** exercise over **observed** journeys. It does **not** by itself answer: *“Would this conversion have happened **without** the ad?”* That causal question belongs to **experiments** and incrementality analysis.

---

## Path and conversion (scope)

A **conversion** is a defined success event (e.g., purchase, signup) with a **timestamp**. A **path** is an ordered sequence of **marketing touchpoints** (impressions, clicks, emails, etc.) that occurred **before** the conversion, within **lookback** and **window** rules agreed in the data contract.

Typical parameters:

- **Lookback:** Maximum time from first touch to conversion (e.g., 30 days).  
- **Click / view windows:** Maximum lag from a specific touch to conversion for that touch to remain on the path (e.g., 7-day click, 1-day view).  

Only touches that pass these rules are **in-path** and eligible for credit.

---

## First-touch attribution

**First-touch** assigns **100%** of conversion credit to the **earliest** eligible touchpoint on the path (by timestamp).

**Strengths:** Highlights **discovery** and upper-funnel channels that start journeys.  
**Weaknesses:** Ignores **assist** touches; can **over-credit** early channels when the last nudge was decisive.

**Use when:** Brand and prospecting narratives, early-funnel budgeting stories (with awareness of bias).

---

## Last-touch attribution

**Last-touch** assigns **100%** of credit to the **last** eligible touch before conversion.

**Strengths:** Simple, aligns with many **default platform** reports; emphasizes **closing** influence.  
**Weaknesses:** **Under-credits** awareness and mid-funnel; multiple channels may each claim “last” in different sessions if paths are fragmented.

**Use when:** Short-cycle retail, performance campaigns where the final click is a reasonable proxy for intent (still not causal).

---

## Linear attribution

**Linear** attribution splits credit **evenly** across all **in-path** touches.

If there are \(k\) touches on the path, each receives \(\dfrac{1}{k}\) of the conversion **value** (or count).

**Strengths:** Acknowledges **multiple contributors** without picking a single winner.  
**Weaknesses:** Treats every touch as **equally important**; can dilute credit across low-impact impressions; sensitive to **path length** and tagging gaps.

**Variants** (not always labeled separately): **time-decay** gives more credit to touches closer to conversion; **position-based** (U-shaped) weights first and last more heavily. This platform’s baseline docs focus on **equal** linear split unless otherwise specified in SQL or service code.

---

## Aggregating to campaign or channel level

For a set of conversions in a period:

- Compute credit **per conversion** under the chosen model.  
- **Sum** fractional credits by **campaign**, **placement**, or **partner**.  
- **Attributed revenue** = sum of (conversion revenue × credit share) for each line.

Revenue must use the same **currency** and **order inclusion rules** as finance or retail reporting where reconciliation is required.

---

## Why attribution differs from incrementality

| Dimension | Attribution | Incrementality (experiments) |
|-----------|-------------|--------------------------------|
| **Question** | How to **allocate credit** among touches we saw? | How much **extra** outcome did media **cause**? |
| **Data** | Observed paths to **converters** | Treatment vs. **control** (or modeled counterfactual) |
| **Causality** | **Descriptive** splitting of a fixed pie | **Comparative** estimate under design assumptions |
| **Overlap** | Many models can **double-count** across channels if summed naively | Designed to attribute **incremental** lift to the test cell |
| **Typical KPIs** | Attributed revenue, **ROAS** | **Lift**, **incremental revenue**, **iROAS** |

**Over-credit scenario:** **Last-touch** or **generous view windows** may assign revenue to an ad that **did not change** the user’s decision; **ROAS** looks strong while **iROAS** from a holdout is weak.

**Under-credit scenario:** **First-touch** may miss the retail media touch that **closed** the sale on the retailer site; **ROAS** looks low while incrementality for that tactic could still be positive.

**Practical takeaway:** Use **attribution** for **mix and operational** storytelling within one consistent framework; use **incrementality** for **budget validation** and **strategic** “did it work?” decisions. Comparing **attributed revenue** to **incremental revenue** (and **ROAS** to **iROAS**) is a deliberate **reconciliation** exercise, not a single number.

---

## References within this repo

- **Definitions:** `kpi_definitions.md` (CTR, ROAS, lift, iROAS, etc.).  
- **Tests:** `experiment_design.md` (holdouts, windows, lift calculation).
