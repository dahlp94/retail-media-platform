"""
Validate campaign metric *logic* (KPI formulas and divide-by-zero behavior).

These tests mirror the SQL in ``sql/marts/*.sql`` using small pandas DataFrames—no database
required. When the project adds a SQL runner, the same golden cases can be reused.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

# --- Helpers mirroring marts SQL semantics (NULL in SQL ~= NaN in tests) ---


def _ratio(numerator: float, denominator: float) -> float:
    """SQL: numerator / NULLIF(denominator, 0) → NaN when denominator is 0."""
    if denominator == 0:
        return float("nan")
    return numerator / denominator


def campaign_base_metrics(ad_events: pd.DataFrame, transactions: pd.DataFrame) -> pd.DataFrame:
    """Mirror ``marts.campaign_base_metrics`` (FULL OUTER grain on campaign_id)."""
    ad = (
        ad_events.groupby("campaign_id", dropna=False)
        .agg(
            impressions=("event_type", lambda s: (s == "impression").sum()),
            clicks=("event_type", lambda s: (s == "click").sum()),
            spend_usd=("cost", "sum"),
        )
        .reset_index()
    )
    ad["spend_usd"] = ad["spend_usd"].fillna(0)

    tx = transactions.dropna(subset=["source_campaign_id"]).copy()
    tx["source_campaign_id"] = tx["source_campaign_id"].astype(int)
    ord_ = (
        tx.groupby("source_campaign_id", as_index=False)
        .agg(orders=("transaction_id", "count"), revenue_usd=("order_value_usd", "sum"))
        .rename(columns={"source_campaign_id": "campaign_id"})
    )

    out = ad.merge(ord_, on="campaign_id", how="outer")
    out["impressions"] = out["impressions"].fillna(0).astype("int64")
    out["clicks"] = out["clicks"].fillna(0).astype("int64")
    out["spend_usd"] = out["spend_usd"].fillna(0)
    out["orders"] = out["orders"].fillna(0).astype("int64")
    out["revenue_usd"] = out["revenue_usd"].fillna(0)
    return out


def campaign_funnel_metrics(base: pd.DataFrame) -> pd.DataFrame:
    """Mirror ``marts.campaign_funnel_metrics``."""
    rows = []
    for _, r in base.iterrows():
        imp, clk, spend, ord_ = (
            int(r["impressions"]),
            int(r["clicks"]),
            float(r["spend_usd"]),
            int(r["orders"]),
        )
        rows.append(
            {
                "campaign_id": r["campaign_id"],
                "impressions": imp,
                "clicks": clk,
                "orders": ord_,
                "ctr": _ratio(clk, imp),
                "cvr": _ratio(ord_, clk),
                "cpc_usd": _ratio(spend, float(clk)),
                "cpo_usd": _ratio(spend, float(ord_)),
            }
        )
    return pd.DataFrame(rows)


def campaign_spend_metrics(base: pd.DataFrame) -> pd.DataFrame:
    """Mirror ``marts.campaign_spend_metrics``."""
    rows = []
    for _, r in base.iterrows():
        imp, clk = int(r["impressions"]), int(r["clicks"])
        spend, rev, ord_ = float(r["spend_usd"]), float(r["revenue_usd"]), int(r["orders"])
        rows.append(
            {
                "campaign_id": r["campaign_id"],
                "spend_usd": spend,
                "revenue_usd": rev,
                "orders": ord_,
                "roas": _ratio(rev, spend),
                "avg_revenue_per_order_usd": _ratio(rev, float(ord_)),
                "avg_spend_per_impression_usd": _ratio(spend, float(imp)),
                "avg_spend_per_click_usd": _ratio(spend, float(clk)),
            }
        )
    return pd.DataFrame(rows)


def executive_overall_roas(base: pd.DataFrame) -> float:
    """Mirror ``marts.executive_summary_metrics.overall_roas`` (ratio of totals)."""
    total_rev = base["revenue_usd"].sum()
    total_spend = base["spend_usd"].sum()
    return _ratio(float(total_rev), float(total_spend))


# --- Golden cases ---


@pytest.fixture
def sample_ad_events() -> pd.DataFrame:
    """Two campaigns: C1 has impressions + clicks + cost; C2 impressions only (CTR denominator test)."""
    return pd.DataFrame(
        {
            "campaign_id": [1, 1, 1, 1, 2, 2],
            "event_type": ["impression", "impression", "click", "click", "impression", "impression"],
            "cost": [0.002, 0.002, 0.50, 0.50, 0.001, 0.001],
        }
    )


@pytest.fixture
def sample_transactions() -> pd.DataFrame:
    """Attributed orders for campaign 1 only."""
    return pd.DataFrame(
        {
            "transaction_id": [101, 102],
            "source_campaign_id": [1, 1],
            "order_value_usd": [40.0, 60.0],
        }
    )


def test_campaign_base_impressions_clicks_spend_orders_revenue(
    sample_ad_events: pd.DataFrame,
    sample_transactions: pd.DataFrame,
) -> None:
    base = campaign_base_metrics(sample_ad_events, sample_transactions)
    c1 = base.loc[base["campaign_id"] == 1].iloc[0]
    assert int(c1["impressions"]) == 2
    assert int(c1["clicks"]) == 2
    assert float(c1["spend_usd"]) == pytest.approx(1.004, rel=1e-9)
    assert int(c1["orders"]) == 2
    assert float(c1["revenue_usd"]) == 100.0

    c2 = base.loc[base["campaign_id"] == 2].iloc[0]
    assert int(c2["impressions"]) == 2
    assert int(c2["clicks"]) == 0
    assert float(c2["spend_usd"]) == pytest.approx(0.002, rel=1e-9)
    assert int(c2["orders"]) == 0
    assert float(c2["revenue_usd"]) == 0.0


def test_funnel_ctr_cvr_cpc_cpo(sample_ad_events: pd.DataFrame, sample_transactions: pd.DataFrame) -> None:
    base = campaign_base_metrics(sample_ad_events, sample_transactions)
    funnel = campaign_funnel_metrics(base).set_index("campaign_id")

    c1 = funnel.loc[1]
    assert c1["ctr"] == pytest.approx(2 / 2)
    assert c1["cvr"] == pytest.approx(2 / 2)
    assert c1["cpc_usd"] == pytest.approx(1.004 / 2)
    assert c1["cpo_usd"] == pytest.approx(1.004 / 2)

    c2 = funnel.loc[2]
    assert c2["ctr"] == pytest.approx(0.0)
    assert math.isnan(c2["cvr"])  # no clicks
    assert math.isnan(c2["cpc_usd"])
    assert math.isnan(c2["cpo_usd"])


def test_spend_roas_and_aov(sample_ad_events: pd.DataFrame, sample_transactions: pd.DataFrame) -> None:
    base = campaign_base_metrics(sample_ad_events, sample_transactions)
    spend = campaign_spend_metrics(base).set_index("campaign_id")

    c1 = spend.loc[1]
    assert c1["roas"] == pytest.approx(100.0 / 1.004)
    assert c1["avg_revenue_per_order_usd"] == pytest.approx(50.0)
    assert c1["avg_spend_per_impression_usd"] == pytest.approx(1.004 / 2)
    assert c1["avg_spend_per_click_usd"] == pytest.approx(1.004 / 2)


def test_divide_by_zero_roas_when_no_spend() -> None:
    """Orders with attribution but zero recorded spend → ROAS undefined (SQL NULL)."""
    ad = pd.DataFrame(
        {
            "campaign_id": [99],
            "event_type": ["impression"],
            "cost": [0.0],
        }
    )
    tx = pd.DataFrame(
        {
            "transaction_id": [1],
            "source_campaign_id": [99],
            "order_value_usd": [25.0],
        }
    )
    base = campaign_base_metrics(ad, tx)
    s = campaign_spend_metrics(base).iloc[0]
    assert math.isnan(s["roas"])


def test_executive_overall_roas_is_ratio_of_totals_not_mean_of_campaign_roas() -> None:
    """
    Platform ROAS must be sum(revenue)/sum(spend), not average(per-campaign ROAS).
    Campaign A: spend 100, revenue 400 → ROAS 4. Campaign B: spend 50, revenue 50 → ROAS 1.
    Overall: 450/150 = 3.0; naive mean of campaign ROAS values (4 + 1) / 2 = 2.5.
    """
    base = pd.DataFrame(
        {
            "campaign_id": [1, 2],
            "impressions": [100, 100],
            "clicks": [10, 10],
            "spend_usd": [100.0, 50.0],
            "orders": [5, 5],
            "revenue_usd": [400.0, 50.0],
        }
    )
    overall = executive_overall_roas(base)
    assert overall == pytest.approx((400 + 50) / (100 + 50))  # 3.0
    naive_mean_roas = ((400 / 100) + (50 / 50)) / 2  # (4 + 1) / 2 = 2.5
    assert overall != pytest.approx(naive_mean_roas)


def test_expected_metric_field_names_document_contract() -> None:
    """Lightweight contract: names align with marts SQL for future integration tests."""
    base_cols = {"campaign_id", "impressions", "clicks", "spend_usd", "orders", "revenue_usd"}
    # Funnel mart carries volumes + ratios only (no spend/revenue columns in SQL output).
    funnel_cols = {"campaign_id", "impressions", "clicks", "orders", "ctr", "cvr", "cpc_usd", "cpo_usd"}
    spend_cols = {
        "campaign_id",
        "spend_usd",
        "revenue_usd",
        "orders",
        "roas",
        "avg_revenue_per_order_usd",
        "avg_spend_per_impression_usd",
        "avg_spend_per_click_usd",
    }

    ad = pd.DataFrame({"campaign_id": [1], "event_type": ["impression"], "cost": [0.01]})
    tx = pd.DataFrame({"transaction_id": [1], "source_campaign_id": [1], "order_value_usd": [10.0]})
    base = campaign_base_metrics(ad, tx)
    assert set(base.columns) == base_cols

    f = campaign_funnel_metrics(base)
    assert set(f.columns) == funnel_cols

    s = campaign_spend_metrics(base)
    assert set(s.columns) == spend_cols
