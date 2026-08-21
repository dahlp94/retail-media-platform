"""
Parity checks between dbt-built marts and the frozen v1 pandas reference.

These tests read PostgreSQL. They skip when the warehouse is unavailable
or when dbt models have not been built yet. They do not regenerate
synthetic source data.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from tests.test_incrementality import campaign_window_member_outcomes, compute_experiment_lift

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC_DIR = REPO_ROOT / "data" / "synthetic"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"

FROZEN_DECISION_COUNTS = {
    "increase_budget": 14,
    "maintain": 3,
    "inconclusive": 7,
}
FROZEN_HEALTH_COUNTS = {"PASS": 22, "WARN": 2}
FROZEN_SEGMENT_ROWS = 1152
FROZEN_SEGMENT_CAMPAIGNS = 24
FROZEN_SEGMENT_SEGMENTS = 5
FROZEN_SEGMENT_GEOS = 48


def _engine_or_skip():
    try:
        from app.core.database import get_engine

        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("select 1"))
        return engine
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"PostgreSQL unavailable: {exc}")


def _require_mart(engine, table: str) -> None:
    inspector = inspect(engine)
    if not inspector.has_table(table, schema="marts"):
        pytest.skip(f"marts.{table} is missing; run `scripts/run_dbt.sh run` first.")


def _require_csv(path: Path) -> None:
    if not path.is_file():
        pytest.skip(f"Missing {path}")


@pytest.fixture(scope="module")
def engine():
    return _engine_or_skip()


@pytest.fixture(scope="module")
def lift_db(engine) -> pd.DataFrame:
    _require_mart(engine, "experiment_lift_metrics")
    return pd.read_sql_table("experiment_lift_metrics", con=engine, schema="marts")


@pytest.fixture(scope="module")
def outcomes_db(engine) -> pd.DataFrame:
    _require_mart(engine, "experiment_member_outcomes")
    return pd.read_sql_table("experiment_member_outcomes", con=engine, schema="marts")


@pytest.fixture(scope="module")
def segment_db(engine) -> pd.DataFrame:
    _require_mart(engine, "segment_performance_metrics")
    return pd.read_sql_table("segment_performance_metrics", con=engine, schema="marts")


@pytest.fixture(scope="module")
def pandas_lift() -> pd.DataFrame:
    assignments_path = SYNTHETIC_DIR / "campaign_experiment_assignments.csv"
    campaigns_path = SYNTHETIC_DIR / "campaigns.csv"
    transactions_path = SYNTHETIC_DIR / "transactions.csv"
    _require_csv(assignments_path)
    _require_csv(campaigns_path)
    _require_csv(transactions_path)
    assignments = pd.read_csv(assignments_path)
    campaigns = pd.read_csv(campaigns_path)
    campaigns["start_date"] = pd.to_datetime(campaigns["start_date"]).dt.date
    campaigns["end_date"] = pd.to_datetime(campaigns["end_date"]).dt.date
    transactions = pd.read_csv(transactions_path)
    return compute_experiment_lift(assignments, campaigns, transactions)


@pytest.fixture(scope="module")
def pandas_outcomes() -> pd.DataFrame:
    assignments_path = SYNTHETIC_DIR / "campaign_experiment_assignments.csv"
    campaigns_path = SYNTHETIC_DIR / "campaigns.csv"
    transactions_path = SYNTHETIC_DIR / "transactions.csv"
    _require_csv(assignments_path)
    _require_csv(campaigns_path)
    _require_csv(transactions_path)
    assignments = pd.read_csv(assignments_path)
    campaigns = pd.read_csv(campaigns_path)
    campaigns["start_date"] = pd.to_datetime(campaigns["start_date"]).dt.date
    campaigns["end_date"] = pd.to_datetime(campaigns["end_date"]).dt.date
    transactions = pd.read_csv(transactions_path)
    return campaign_window_member_outcomes(assignments, campaigns, transactions)


class TestDbtLiftParity:
    def test_campaign_set_matches(self, lift_db: pd.DataFrame, pandas_lift: pd.DataFrame) -> None:
        assert set(lift_db["campaign_id"]) == set(pandas_lift["campaign_id"])
        assert len(lift_db) == 24

    def test_point_estimates_match_pandas_reference(
        self, lift_db: pd.DataFrame, pandas_lift: pd.DataFrame
    ) -> None:
        left = lift_db.set_index("campaign_id").sort_index()
        right = pandas_lift.set_index("campaign_id").sort_index()
        for campaign_id in left.index:
            db_row = left.loc[campaign_id]
            py_row = right.loc[campaign_id]
            assert int(db_row["treatment_member_count"]) == int(py_row["n_members_t"])
            assert int(db_row["control_member_count"]) == int(py_row["n_members_c"])
            assert int(db_row["treatment_converters"]) == int(py_row["converters_t"])
            assert int(db_row["control_converters"]) == int(py_row["converters_c"])
            assert db_row["treatment_conversion_rate"] == pytest.approx(
                float(py_row["conversion_rate_t"]), rel=1e-6, abs=1e-8
            )
            assert db_row["absolute_lift"] == pytest.approx(
                float(py_row["absolute_lift"]), rel=1e-6, abs=1e-8
            )
            assert db_row["incremental_orders"] == pytest.approx(
                float(py_row["incremental_orders"]), rel=1e-6, abs=0.001
            )
            assert db_row["incremental_revenue"] == pytest.approx(
                float(py_row["incremental_revenue"]), rel=1e-6, abs=0.02
            )


class TestDbtMemberOutcomeParity:
    def test_grain_is_campaign_member(
        self, outcomes_db: pd.DataFrame, pandas_outcomes: pd.DataFrame
    ) -> None:
        assert not outcomes_db.duplicated(["campaign_id", "member_id"]).any()
        assert len(outcomes_db) == len(pandas_outcomes)
        assert set(zip(outcomes_db["campaign_id"], outcomes_db["member_id"])) == set(
            zip(pandas_outcomes["campaign_id"], pandas_outcomes["member_id"])
        )

    def test_converters_and_orders_match(
        self, outcomes_db: pd.DataFrame, pandas_outcomes: pd.DataFrame
    ) -> None:
        left = outcomes_db.sort_values(["campaign_id", "member_id"]).reset_index(drop=True)
        right = pandas_outcomes.sort_values(["campaign_id", "member_id"]).reset_index(drop=True)
        assert (left["experiment_arm"].astype(str).values == right["experiment_arm"].astype(str).values).all()
        assert (left["is_converter"].astype(int).values == right["is_converter"].astype(int).values).all()
        assert (left["order_count"].astype(int).values == right["order_cnt"].astype(int).values).all()
        revenue_gap = (left["revenue_usd"].astype(float) - right["revenue_usd"].astype(float)).abs()
        assert (revenue_gap <= 0.02).all()

    def test_zero_purchase_members_remain(self, outcomes_db: pd.DataFrame) -> None:
        zeros = outcomes_db.loc[outcomes_db["order_count"].eq(0)]
        assert len(zeros) > 0
        assert zeros["revenue_usd"].eq(0).all()
        assert zeros["is_converter"].eq(0).all()


class TestDbtSegmentParity:
    def test_seed0_shape(self, segment_db: pd.DataFrame) -> None:
        assert len(segment_db) == FROZEN_SEGMENT_ROWS
        assert segment_db["campaign_id"].nunique() == FROZEN_SEGMENT_CAMPAIGNS
        assert segment_db["audience_segment_id"].nunique() == FROZEN_SEGMENT_SEGMENTS
        assert segment_db["primary_geo_id"].nunique() == FROZEN_SEGMENT_GEOS

    def test_reconciles_to_lift(self, segment_db: pd.DataFrame, lift_db: pd.DataFrame) -> None:
        totals = segment_db.groupby("campaign_id", as_index=False).agg(
            treatment_member_count=("treatment_member_count", "sum"),
            control_member_count=("control_member_count", "sum"),
            treatment_converters=("treatment_converters", "sum"),
            control_converters=("control_converters", "sum"),
            treatment_orders=("treatment_orders", "sum"),
            control_orders=("control_orders", "sum"),
            treatment_revenue=("treatment_revenue", "sum"),
            control_revenue=("control_revenue", "sum"),
        )
        merged = totals.merge(lift_db, on="campaign_id", suffixes=("_seg", "_lift"))
        assert (merged["treatment_member_count_seg"] == merged["treatment_member_count_lift"]).all()
        assert (merged["control_member_count_seg"] == merged["control_member_count_lift"]).all()
        assert (merged["treatment_converters_seg"] == merged["treatment_converters_lift"]).all()
        assert (merged["control_converters_seg"] == merged["control_converters_lift"]).all()
        assert (merged["treatment_orders_seg"] == merged["treatment_orders_lift"]).all()
        assert (merged["control_orders_seg"] == merged["control_orders_lift"]).all()
        assert (
            (merged["treatment_revenue_seg"] - merged["treatment_revenue_lift"]).abs() <= 0.05
        ).all()


class TestFrozenStage4Outputs:
    def test_processed_decision_distribution(self) -> None:
        path = PROCESSED_DIR / "campaign_measurement_decisions.csv"
        _require_csv(path)
        dec = pd.read_csv(path)
        counts = dec["measurement_decision"].value_counts().to_dict()
        for key, expected in FROZEN_DECISION_COUNTS.items():
            assert counts.get(key, 0) == expected
        assert counts.get("reduce_budget", 0) == 0
        assert counts.get("do_not_interpret", 0) == 0

    def test_processed_health_distribution(self) -> None:
        path = PROCESSED_DIR / "experiment_health_metrics.csv"
        _require_csv(path)
        health = pd.read_csv(path)
        counts = health["experiment_health_status"].value_counts().to_dict()
        for key, expected in FROZEN_HEALTH_COUNTS.items():
            assert counts.get(key, 0) == expected
        assert counts.get("FAIL", 0) == 0

    def test_campaign_24_attribution_gap(self) -> None:
        path = PROCESSED_DIR / "campaign_measurement_decisions.csv"
        _require_csv(path)
        row = pd.read_csv(path).query("campaign_id == 24").iloc[0]
        assert row["measurement_decision"] == "maintain"
        assert float(row["roas"]) == pytest.approx(1.217, abs=0.01)
        assert float(row["iroas"]) == pytest.approx(0.903, abs=0.01)
        assert row["attribution_incrementality_alignment"] == (
            "attribution_stronger_than_incrementality"
        )
