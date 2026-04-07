"""
Validate incrementality *logic* for Week 6 marts (treatment vs control lift and RCT scaling).

Mirrors ``sql/marts/experiment_lift_metrics.sql`` using small pandas DataFrames—no database.
Assumes all synthetic transactions in the frame count as in-window attributed orders (the SQL layer
also restricts ``order_timestamp`` to campaign dates; tests use data that satisfy that intent).

Output columns match ``marts.experiment_lift_metrics``:
``campaign_id``, ``treatment_member_count``, ``control_member_count``, ``treatment_conversion_rate``,
``control_conversion_rate``, ``absolute_lift``, ``relative_lift``, ``incremental_orders``,
``incremental_revenue``.
"""
import math

import pandas as pd
import pytest


def compute_experiment_lift(assignments: pd.DataFrame,
                            campaigns: pd.DataFrame,
                            transactions: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame-based reference implementation of experiment lift logic.

    Design:
    - campaign membership comes from experiment assignment
    - outcome window comes from campaign start/end dates
    - outcomes use all transactions by assigned members during the campaign window
    - source_campaign_id is intentionally ignored for causal outcome definition
    """
    experiment_population = assignments.merge(
        campaigns[["campaign_id", "start_date", "end_date"]],
        on="campaign_id",
        how="inner",
    )

    experiment_population = experiment_population[
        experiment_population["experiment_arm"].isin(["treatment", "control"])
    ].copy()

    tx = transactions.copy()
    tx["order_date"] = pd.to_datetime(tx["order_timestamp"]).dt.date

    pop_tx = experiment_population.merge(
        tx,
        on="member_id",
        how="left",
        suffixes=("", "_tx"),
    )

    in_window = (
        pop_tx["order_date"].notna()
        & (pop_tx["order_date"] >= pop_tx["start_date"])
        & (pop_tx["order_date"] <= pop_tx["end_date"])
    )

    pop_tx["txn_in_window"] = in_window.astype(int)
    pop_tx["order_value_in_window"] = pop_tx["order_value_usd"].where(in_window, 0.0)
    pop_tx["transaction_id_in_window"] = pop_tx["transaction_id"].where(in_window)

    member_outcomes = (
        pop_tx.groupby(["campaign_id", "member_id", "experiment_arm"], as_index=False)
        .agg(
            order_cnt=("txn_in_window", "sum"),
            revenue_usd=("order_value_in_window", "sum"),
        )
    )

    member_outcomes["is_converter"] = (member_outcomes["order_cnt"] > 0).astype(int)

    arm_agg = (
        member_outcomes.groupby(["campaign_id", "experiment_arm"], as_index=False)
        .agg(
            n_members=("member_id", "count"),
            converters=("is_converter", "sum"),
            total_orders=("order_cnt", "sum"),
            total_revenue=("revenue_usd", "sum"),
        )
    )

    arm_agg["conversion_rate"] = arm_agg["converters"] / arm_agg["n_members"]
    arm_agg["orders_per_member"] = arm_agg["total_orders"] / arm_agg["n_members"]
    arm_agg["revenue_per_member"] = arm_agg["total_revenue"] / arm_agg["n_members"]

    treatment = arm_agg[arm_agg["experiment_arm"] == "treatment"].copy()
    control = arm_agg[arm_agg["experiment_arm"] == "control"].copy()

    result = treatment.merge(control, on="campaign_id", suffixes=("_t", "_c"))

    result["absolute_lift"] = (
        result["conversion_rate_t"] - result["conversion_rate_c"]
    )
    result["relative_lift"] = result["absolute_lift"] / result["conversion_rate_c"]
    result["incremental_orders_per_member"] = (
        result["orders_per_member_t"] - result["orders_per_member_c"]
    )
    result["incremental_revenue_per_member"] = (
        result["revenue_per_member_t"] - result["revenue_per_member_c"]
    )
    result["incremental_orders"] = (
        result["n_members_t"] * result["incremental_orders_per_member"]
    )
    result["incremental_revenue"] = (
        result["n_members_t"] * result["incremental_revenue_per_member"]
    )

    return result


@pytest.fixture
def base_campaigns() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "campaign_id": [101],
            "start_date": [pd.to_datetime("2026-01-01").date()],
            "end_date": [pd.to_datetime("2026-01-07").date()],
        }
    )


def test_incrementality_uses_all_transactions_in_campaign_window_not_source_campaign_id(base_campaigns):
    assignments = pd.DataFrame(
        {
            "campaign_id": [101, 101, 101, 101],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
        }
    )

    transactions = pd.DataFrame(
        {
            "transaction_id": [1001, 1002],
            "member_id": [1, 3],
            "order_timestamp": [
                "2026-01-03 10:00:00",
                "2026-01-04 12:00:00",
            ],
            "order_value_usd": [50.0, 30.0],
            # deliberately not aligned to campaign_id to prove attribution is ignored
            "source_campaign_id": [999, None],
        }
    )

    result = compute_experiment_lift(assignments, base_campaigns, transactions)
    row = result.iloc[0]

    assert row["converters_t"] == 1
    assert row["converters_c"] == 1
    assert row["conversion_rate_t"] == pytest.approx(0.5)
    assert row["conversion_rate_c"] == pytest.approx(0.5)
    assert row["absolute_lift"] == pytest.approx(0.0)
    assert row["incremental_revenue"] == pytest.approx(20.0)


def test_out_of_window_transactions_do_not_count(base_campaigns):
    assignments = pd.DataFrame(
        {
            "campaign_id": [101, 101],
            "member_id": [1, 2],
            "experiment_arm": ["treatment", "control"],
        }
    )

    transactions = pd.DataFrame(
        {
            "transaction_id": [1001, 1002],
            "member_id": [1, 2],
            "order_timestamp": [
                "2025-12-31 23:59:59",  # before window
                "2026-01-08 00:00:00",  # after window
            ],
            "order_value_usd": [100.0, 120.0],
            "source_campaign_id": [101, 101],
        }
    )

    result = compute_experiment_lift(assignments, base_campaigns, transactions)
    row = result.iloc[0]

    assert row["converters_t"] == 0
    assert row["converters_c"] == 0
    assert row["total_orders_t"] == 0
    assert row["total_orders_c"] == 0
    assert row["total_revenue_t"] == 0.0
    assert row["total_revenue_c"] == 0.0


def test_incremental_orders_and_revenue_are_scaled_from_per_member_differences(base_campaigns):
    assignments = pd.DataFrame(
        {
            "campaign_id": [101, 101, 101, 101],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
        }
    )

    transactions = pd.DataFrame(
        {
            "transaction_id": [1001, 1002, 1003, 1004],
            "member_id": [1, 1, 2, 3],
            "order_timestamp": [
                "2026-01-02 09:00:00",
                "2026-01-03 09:00:00",
                "2026-01-04 09:00:00",
                "2026-01-05 09:00:00",
            ],
            "order_value_usd": [10.0, 20.0, 30.0, 15.0],
            "source_campaign_id": [None, None, None, None],
        }
    )

    result = compute_experiment_lift(assignments, base_campaigns, transactions)
    row = result.iloc[0]

    # treatment: 3 orders across 2 members => 1.5 orders/member
    # control:   1 order  across 2 members => 0.5 orders/member
    # incremental_orders = 2 * (1.5 - 0.5) = 2.0
    assert row["orders_per_member_t"] == pytest.approx(1.5)
    assert row["orders_per_member_c"] == pytest.approx(0.5)
    assert row["incremental_orders"] == pytest.approx(2.0)

    # treatment revenue = 60 => 30/member
    # control revenue   = 15 => 7.5/member
    # incremental_revenue = 2 * (30 - 7.5) = 45
    assert row["revenue_per_member_t"] == pytest.approx(30.0)
    assert row["revenue_per_member_c"] == pytest.approx(7.5)
    assert row["incremental_revenue"] == pytest.approx(45.0)


def test_absolute_and_relative_lift_are_computed_correctly(base_campaigns):
    assignments = pd.DataFrame(
        {
            "campaign_id": [101, 101, 101, 101],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
        }
    )

    transactions = pd.DataFrame(
        {
            "transaction_id": [1001, 1002, 1003],
            "member_id": [1, 2, 3],
            "order_timestamp": [
                "2026-01-02 10:00:00",
                "2026-01-03 10:00:00",
                "2026-01-04 10:00:00",
            ],
            "order_value_usd": [20.0, 25.0, 15.0],
            "source_campaign_id": [101, 101, 101],
        }
    )

    result = compute_experiment_lift(assignments, base_campaigns, transactions)
    row = result.iloc[0]

    # treatment conversion = 2/2 = 1.0
    # control conversion = 1/2 = 0.5
    # absolute lift = 0.5
    # relative lift = 1.0
    assert row["conversion_rate_t"] == pytest.approx(1.0)
    assert row["conversion_rate_c"] == pytest.approx(0.5)
    assert row["absolute_lift"] == pytest.approx(0.5)
    assert row["relative_lift"] == pytest.approx(1.0)


def test_source_campaign_id_does_not_change_member_outcome_definition(base_campaigns):
    assignments = pd.DataFrame(
        {
            "campaign_id": [101, 101, 101, 101],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
        }
    )

    transactions_a = pd.DataFrame(
        {
            "transaction_id": [1001, 1002],
            "member_id": [1, 3],
            "order_timestamp": [
                "2026-01-03 10:00:00",
                "2026-01-03 11:00:00",
            ],
            "order_value_usd": [40.0, 25.0],
            "source_campaign_id": [101, 101],
        }
    )

    transactions_b = transactions_a.copy()
    transactions_b["source_campaign_id"] = [999, None]

    result_a = compute_experiment_lift(assignments, base_campaigns, transactions_a)
    result_b = compute_experiment_lift(assignments, base_campaigns, transactions_b)

    row_a = result_a.iloc[0]
    row_b = result_b.iloc[0]

    compare_cols = [
        "converters_t",
        "converters_c",
        "total_orders_t",
        "total_orders_c",
        "total_revenue_t",
        "total_revenue_c",
        "conversion_rate_t",
        "conversion_rate_c",
        "absolute_lift",
        "incremental_orders",
        "incremental_revenue",
    ]

    for col in compare_cols:
        assert row_a[col] == pytest.approx(row_b[col])


def test_zero_control_conversion_produces_infinite_relative_lift_in_reference_logic(base_campaigns):
    """
    The SQL implementation should return NULL when control conversion rate is zero.
    This reference test checks the underlying scenario and makes the edge case explicit.
    """
    assignments = pd.DataFrame(
        {
            "campaign_id": [101, 101, 101, 101],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
        }
    )

    transactions = pd.DataFrame(
        {
            "transaction_id": [1001],
            "member_id": [1],
            "order_timestamp": ["2026-01-03 10:00:00"],
            "order_value_usd": [50.0],
            "source_campaign_id": [None],
        }
    )

    result = compute_experiment_lift(assignments, base_campaigns, transactions)
    row = result.iloc[0]

    assert row["conversion_rate_t"] == pytest.approx(0.5)
    assert row["conversion_rate_c"] == pytest.approx(0.0)
    assert row["absolute_lift"] == pytest.approx(0.5)
    assert math.isinf(row["relative_lift"])


def test_member_with_multiple_transactions_counts_once_as_converter(base_campaigns):
    assignments = pd.DataFrame(
        {
            "campaign_id": [101, 101, 101, 101],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
        }
    )

    transactions = pd.DataFrame(
        {
            "transaction_id": [1001, 1002, 1003],
            "member_id": [1, 1, 3],
            "order_timestamp": [
                "2026-01-02 10:00:00",
                "2026-01-03 10:00:00",
                "2026-01-04 10:00:00",
            ],
            "order_value_usd": [10.0, 12.0, 8.0],
            "source_campaign_id": [None, None, None],
        }
    )

    result = compute_experiment_lift(assignments, base_campaigns, transactions)
    row = result.iloc[0]

    assert row["converters_t"] == 1
    assert row["converters_c"] == 1
    assert row["total_orders_t"] == 2
    assert row["total_orders_c"] == 1