# Retail Media Measurement, Experimentation, and Incrementality Platform

A **production-style** analytics and decision-support system for retail media. The platform simulates advertiser campaign performance, measures **attributed** and **incremental** impact, and surfaces dashboard-ready insights plus recommendation-oriented outputs.

---

## Project overview

This repository models how retail media teams answer causal and allocation questions: whether ads actually drove purchases, how segments respond, how attribution compares to experimentally measured lift, and where budget should move. Synthetic data keeps the project self-contained and reproducible while the architecture mirrors real pipelines (metrics layer, attribution, experiments, APIs, and optional ML).

---

## Business problem

Retail media networks and advertisers routinely need to:

- **Causality:** Did a campaign cause incremental purchases, or would buyers have converted anyway?
- **Segmentation:** Which audiences respond best to which tactics?
- **Reconciliation:** How does rule- or model-based **attributed revenue** compare to **lift** from holdouts or geo experiments?
- **Budget:** Which campaigns deserve more spend, and which look strong in attribution but weak on true incremental impact?

This project implements the data, metrics, and analytical layers needed to explore those questions end-to-end.

---

## Architecture (high level)

```text
Synthetic data  →  Staging / marts (SQL)  →  Attribution engine
                              ↓
                    Incrementality & lift analysis
                              ↓
              API  +  Dashboard  +  Recommendations  (+ optional ML)
```

- **Data:** Controlled synthetic generator for impressions, events, orders, and experiments.
- **SQL layer:** Staging tables and marts for campaign and experiment KPIs.
- **Attribution:** Rules or models that assign credit along the path to purchase.
- **Incrementality:** Holdout / test designs and lift estimation vs. attribution.
- **Serving:** FastAPI for programmatic access; Streamlit for exploration; optional MLflow for experiment tracking.

---

## Tech stack

| Area | Choice |
|------|--------|
| Language | Python 3.11+ |
| Database | PostgreSQL |
| Analytics | Pandas, NumPy, SciPy, scikit-learn |
| API | FastAPI, Uvicorn |
| Dashboards | Streamlit |
| ML ops (optional / near-term) | MLflow |
| Access to Postgres | SQLAlchemy, psycopg2 |

---

## Repository structure

```text
retail-media-platform/
├── app/
│   ├── api/
│   ├── core/
│   └── services/
├── configs/
├── data/
├── dashboards/
├── docs/
├── models/
├── notebooks/
├── scripts/
├── sql/
│   ├── staging/
│   ├── marts/
│   └── attribution/
├── tests/
├── .env.example
├── README.md
├── requirements.txt
└── run_pipeline.py
```

---

## Planned phases

1. **Foundation:** Synthetic retail media data generator; seed data layout; environment and docs.
2. **Metrics layer:** PostgreSQL schemas, staging → marts SQL, core campaign and experiment KPIs.
3. **Attribution:** Attribution logic and `sql/attribution` artifacts aligned with the metric layer.
4. **Incrementality:** Lift analysis, holdout / pseudo-experiment flows, attribution vs. lift comparison.
5. **Product surface:** FastAPI endpoints and Streamlit dashboards consuming marts and analysis outputs.
6. **Recommendations:** Rule- and score-based budget / campaign hints from combined signals.
7. **Optional ML:** Propensity / uplift-style extensions behind clear interfaces, tracked with MLflow where useful.

Phases are implemented incrementally; components that do not exist yet use **minimal placeholders** until their phase is built.

---

## Getting started (preview)

1. Copy `.env.example` to `.env` and set PostgreSQL and app variables.
2. Create a virtual environment and install dependencies: `pip install -r requirements.txt`.
3. Later steps will document database initialization, pipeline runs (`run_pipeline.py`), and how to start the API and dashboard.

Details will expand as each phase lands.

---

## License

Use and modify for portfolio or learning purposes unless otherwise specified.
