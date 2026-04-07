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

from __future__ import annotations

import math

import pandas as pd
import pytest

# --- Helpers mirroring ``marts.experiment_lift_metrics`` (SQL NULL ~= float NaN in assertions) ---


def _rate(converters: int, n_members: int) -> float:
    """converters / NULLIF(n_members, 0)."""
    if n_members == 0:
        return float("nan")
    return converters / n_members


def experiment_lift_metrics(
    assignments: pd.DataFrame,
    transactions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Mirror campaign-level ``experiment_lift_metrics`` mart logic.

    Parameters
    ----------
    assignments
        Columns: ``campaign_id``, ``member_id``, ``experiment_arm`` (``treatment`` / ``control``).
    transactions
        Attributed orders: ``member_id``, ``source_campaign_id``, ``order_value_usd``.
        Each row is one order; ``source_campaign_id`` links to the campaign (matches SQL grain).
    """
    required_a = {"campaign_id", "member_id", "experiment_arm"}
    if not required_a.issubset(assignments.columns):
        raise ValueError(f"assignments missing columns: {required_a - set(assignments.columns)}")

    tx = transactions.dropna(subset=["source_campaign_id"]).copy()
    if tx.empty:
        attributed = pd.DataFrame(
            columns=["member_id", "campaign_id", "order_cnt", "revenue_usd"],
        )
    else:
        tx["campaign_id"] = tx["source_campaign_id"].astype(int)
        attributed = (
            tx.groupby(["member_id", "campaign_id"], as_index=False)
            .agg(
                order_cnt=("order_value_usd", "size"),
                revenue_usd=("order_value_usd", "sum"),
            )
        )

    a = assignments.loc[assignments["experiment_arm"].isin(["treatment", "control"])].copy()
    ml = a.merge(attributed, on=["member_id", "campaign_id"], how="left")
    ml["order_cnt"] = ml["order_cnt"].fillna(0)
    ml["revenue_usd"] = ml["revenue_usd"].fillna(0.0)
    ml["is_converter"] = (ml["order_cnt"] > 0).astype(int)

    arm = (
        ml.groupby(["campaign_id", "experiment_arm"], as_index=False)
        .agg(
            n_members=("member_id", "count"),
            converters=("is_converter", "sum"),
            total_orders=("order_cnt", "sum"),
            total_revenue=("revenue_usd", "sum"),
        )
    )

    rows: list[dict] = []
    for campaign_id in arm["campaign_id"].unique():
        g = arm.loc[arm["campaign_id"] == campaign_id]
        t = g.loc[g["experiment_arm"] == "treatment"]
        c = g.loc[g["experiment_arm"] == "control"]

        n_t = int(t["n_members"].iloc[0]) if len(t) else 0
        n_c = int(c["n_members"].iloc[0]) if len(c) else 0
        conv_t = int(t["converters"].iloc[0]) if len(t) else 0
        conv_c = int(c["converters"].iloc[0]) if len(c) else 0
        ord_t = int(t["total_orders"].iloc[0]) if len(t) else 0
        ord_c = int(c["total_orders"].iloc[0]) if len(c) else 0
        rev_t = float(t["total_revenue"].iloc[0]) if len(t) else 0.0
        rev_c = float(c["total_revenue"].iloc[0]) if len(c) else 0.0

        cvr_t = _rate(conv_t, n_t)
        cvr_c = _rate(conv_c, n_c)

        # NaN propagates like SQL NULL for (treatment_cvr - control_cvr).
        abs_lift = cvr_t - cvr_c

        if math.isnan(cvr_c) or cvr_c == 0.0:
            rel_lift = float("nan")
        elif math.isnan(abs_lift):
            rel_lift = float("nan")
        else:
            rel_lift = abs_lift / cvr_c

        if n_t > 0 and n_c > 0:
            inc_ord = ord_t - n_t * (ord_c / n_c)
            inc_rev = rev_t - n_t * (rev_c / n_c)
        else:
            inc_ord = float("nan")
            inc_rev = float("nan")

        rows.append(
            {
                "campaign_id": int(campaign_id),
                "treatment_member_count": n_t,
                "control_member_count": n_c,
                "treatment_conversion_rate": cvr_t,
                "control_conversion_rate": cvr_c,
                "absolute_lift": abs_lift,
                "relative_lift": rel_lift,
                "incremental_orders": inc_ord,
                "incremental_revenue": inc_rev,
            }
        )

    return pd.DataFrame(rows)


# --- Golden cases ---


def test_balanced_arms_lift_and_incremental_orders_revenue() -> None:
    """
    Campaign 1: 4 treatment (2 converters, 3 orders, $30), 4 control (1 converter, 1 order, $10).

    Control CVR = 1/4 = 0.25; treatment CVR = 2/4 = 0.5.
    incremental_orders = 3 - 4*(1/4) = 2; incremental_revenue = 30 - 4*(10/4) = 20.
    """
    assignments = pd.DataFrame(
        {
            "campaign_id": [1, 1, 1, 1, 1, 1, 1, 1],
            "member_id": [101, 102, 103, 104, 201, 202, 203, 204],
            "experiment_arm": ["treatment"] * 4 + ["control"] * 4,
        }
    )
    transactions = pd.DataFrame(
        {
            "member_id": [101, 102, 102, 203],
            "source_campaign_id": [1, 1, 1, 1],
            "order_value_usd": [10.0, 10.0, 10.0, 10.0],
        }
    )
    m = experiment_lift_metrics(assignments, transactions).set_index("campaign_id").loc[1]

    assert m["treatment_member_count"] == 4
    assert m["control_member_count"] == 4
    assert m["treatment_conversion_rate"] == pytest.approx(0.5)
    assert m["control_conversion_rate"] == pytest.approx(0.25)
    assert m["absolute_lift"] == pytest.approx(0.25)
    assert m["relative_lift"] == pytest.approx(1.0)  # 0.25 / 0.25
    assert m["incremental_orders"] == pytest.approx(2.0)
    assert m["incremental_revenue"] == pytest.approx(20.0)


def test_zero_control_conversions_relative_lift_undefined() -> None:
    """Control CVR = 0 → relative lift divides by NULL in SQL; incremental math still defined."""
    assignments = pd.DataFrame(
        {
            "campaign_id": [1, 1, 1, 1],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
        }
    )
    transactions = pd.DataFrame(
        {
            "member_id": [1],
            "source_campaign_id": [1],
            "order_value_usd": [50.0],
        }
    )
    m = experiment_lift_metrics(assignments, transactions).iloc[0]

    assert m["control_conversion_rate"] == pytest.approx(0.0)
    assert m["treatment_conversion_rate"] == pytest.approx(0.5)
    assert m["absolute_lift"] == pytest.approx(0.5)
    assert math.isnan(m["relative_lift"])
    # orders: T=1, C=0, n_t=n_c=2 → 1 - 2*(0/2) = 1
    assert m["incremental_orders"] == pytest.approx(1.0)
    assert m["incremental_revenue"] == pytest.approx(50.0)


def test_zero_treatment_conversions_negative_absolute_lift() -> None:
    """Treatment has no attributed orders; control has some → lift negative; relative lift defined."""
    assignments = pd.DataFrame(
        {
            "campaign_id": [1, 1, 1, 1],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
        }
    )
    transactions = pd.DataFrame(
        {
            "member_id": [3, 4],
            "source_campaign_id": [1, 1],
            "order_value_usd": [20.0, 30.0],
        }
    )
    m = experiment_lift_metrics(assignments, transactions).iloc[0]

    assert m["treatment_conversion_rate"] == pytest.approx(0.0)
    assert m["control_conversion_rate"] == pytest.approx(1.0)
    assert m["absolute_lift"] == pytest.approx(-1.0)
    assert m["relative_lift"] == pytest.approx(-1.0)
    assert m["incremental_orders"] == pytest.approx(-2.0)
    assert m["incremental_revenue"] == pytest.approx(-50.0)


def test_zero_treatment_members_makes_rates_and_incremental_undefined() -> None:
    """No treatment assignments: CVR undefined; incremental NULL (needs both n_t and n_c > 0)."""
    assignments = pd.DataFrame(
        {
            "campaign_id": [1, 1],
            "member_id": [10, 11],
            "experiment_arm": ["control", "control"],
        }
    )
    transactions = pd.DataFrame(
        {
            "member_id": [10],
            "source_campaign_id": [1],
            "order_value_usd": [15.0],
        }
    )
    m = experiment_lift_metrics(assignments, transactions).iloc[0]

    assert m["treatment_member_count"] == 0
    assert m["control_member_count"] == 2
    assert math.isnan(m["treatment_conversion_rate"])
    assert m["control_conversion_rate"] == pytest.approx(0.5)
    assert math.isnan(m["absolute_lift"])
    assert math.isnan(m["relative_lift"])
    assert math.isnan(m["incremental_orders"])
    assert math.isnan(m["incremental_revenue"])


def test_zero_control_members_makes_control_cvr_and_incremental_undefined() -> None:
    """No control bucket: cannot scale incremental vs control mean."""
    assignments = pd.DataFrame(
        {
            "campaign_id": [1, 1],
            "member_id": [1, 2],
            "experiment_arm": ["treatment", "treatment"],
        }
    )
    transactions = pd.DataFrame(
        {
            "member_id": [1],
            "source_campaign_id": [1],
            "order_value_usd": [40.0],
        }
    )
    m = experiment_lift_metrics(assignments, transactions).iloc[0]

    assert m["treatment_member_count"] == 2
    assert m["control_member_count"] == 0
    assert m["treatment_conversion_rate"] == pytest.approx(0.5)
    assert math.isnan(m["control_conversion_rate"])
    assert math.isnan(m["absolute_lift"])
    assert math.isnan(m["relative_lift"])
    assert math.isnan(m["incremental_orders"])
    assert math.isnan(m["incremental_revenue"])


def test_expected_experiment_lift_metric_column_contract() -> None:
    """Column names align with ``marts.experiment_lift_metrics`` for future DB-backed tests."""
    assignments = pd.DataFrame(
        {"campaign_id": [1, 1], "member_id": [1, 2], "experiment_arm": ["treatment", "control"]}
    )
    transactions = pd.DataFrame(
        {"member_id": [1], "source_campaign_id": [1], "order_value_usd": [10.0]}
    )
    out = experiment_lift_metrics(assignments, transactions)
    assert set(out.columns) == {
        "campaign_id",
        "treatment_member_count",
        "control_member_count",
        "treatment_conversion_rate",
        "control_conversion_rate",
        "absolute_lift",
        "relative_lift",
        "incremental_orders",
        "incremental_revenue",
    }
