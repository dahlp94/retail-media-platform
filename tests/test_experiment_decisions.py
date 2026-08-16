"""
Tests for iROAS transformation and health-aware measurement decisions.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from app.statistics.experiment_decisions import (
    ALIGNMENT_ALIGNED_POSITIVE,
    ALIGNMENT_ATTRIBUTION_STRONGER,
    ALIGNMENT_INCREMENTALITY_STRONGER,
    ALIGNMENT_INCONCLUSIVE,
    ALIGNMENT_NOT_INTERPRETABLE,
    DECISION_DO_NOT_INTERPRET,
    DECISION_INCONCLUSIVE,
    DECISION_INCREASE_BUDGET,
    DECISION_MAINTAIN,
    DECISION_REDUCE_BUDGET,
    build_campaign_decisions,
    classify_alignment,
    compute_iroas,
    decide_measurement,
)
from app.statistics.experiment_health import HEALTH_STATUS_FAIL, HEALTH_STATUS_PASS


class TestIncrementalROAS:
    def test_point_estimate_is_incremental_revenue_over_spend(self) -> None:
        result = compute_iroas(200.0, 100.0, 150.0, 250.0)
        assert result["iroas"] == pytest.approx(2.0)
        assert result["iroas_ci_lower"] == pytest.approx(1.5)
        assert result["iroas_ci_upper"] == pytest.approx(2.5)

    def test_interval_is_deterministic_transformation_of_revenue_ci(self) -> None:
        spend = 40.0
        result = compute_iroas(80.0, spend, 20.0, 140.0)
        assert result["iroas_ci_lower"] == pytest.approx(20.0 / spend)
        assert result["iroas_ci_upper"] == pytest.approx(140.0 / spend)

    def test_zero_spend_returns_nan(self) -> None:
        result = compute_iroas(100.0, 0.0, 50.0, 150.0)
        assert math.isnan(result["iroas"])
        assert math.isnan(result["iroas_ci_lower"])
        assert math.isnan(result["iroas_ci_upper"])

    def test_negative_spend_returns_nan(self) -> None:
        result = compute_iroas(100.0, -5.0, 50.0, 150.0)
        assert math.isnan(result["iroas"])

    def test_missing_revenue_returns_nan(self) -> None:
        result = compute_iroas(float("nan"), 100.0, 10.0, 20.0)
        assert math.isnan(result["iroas"])


class TestDecisionLogic:
    def test_health_failure_withholds_interpretation(self) -> None:
        decision, reason = decide_measurement(
            health_status=HEALTH_STATUS_FAIL,
            incremental_revenue=5000.0,
            incremental_revenue_ci_lower=3000.0,
            incremental_revenue_ci_upper=7000.0,
            iroas=2.5,
        )
        assert decision == DECISION_DO_NOT_INTERPRET
        assert "withhold" in reason

    def test_clearly_positive_with_efficient_iroas_scales(self) -> None:
        decision, reason = decide_measurement(
            health_status=HEALTH_STATUS_PASS,
            incremental_revenue=4000.0,
            incremental_revenue_ci_lower=1500.0,
            incremental_revenue_ci_upper=6500.0,
            iroas=1.8,
        )
        assert decision == DECISION_INCREASE_BUDGET
        assert "iroas" in reason

    def test_clearly_positive_but_weak_iroas_maintains(self) -> None:
        decision, _ = decide_measurement(
            health_status=HEALTH_STATUS_PASS,
            incremental_revenue=800.0,
            incremental_revenue_ci_lower=100.0,
            incremental_revenue_ci_upper=1500.0,
            iroas=0.4,
        )
        assert decision == DECISION_MAINTAIN

    def test_clearly_negative_reduces(self) -> None:
        decision, reason = decide_measurement(
            health_status=HEALTH_STATUS_PASS,
            incremental_revenue=-900.0,
            incremental_revenue_ci_lower=-1600.0,
            incremental_revenue_ci_upper=-200.0,
            iroas=-0.5,
        )
        assert decision == DECISION_REDUCE_BUDGET
        assert "negative" in reason

    def test_interval_crossing_zero_is_inconclusive(self) -> None:
        decision, reason = decide_measurement(
            health_status=HEALTH_STATUS_PASS,
            incremental_revenue=400.0,
            incremental_revenue_ci_lower=-200.0,
            incremental_revenue_ci_upper=1000.0,
            iroas=0.3,
        )
        assert decision == DECISION_INCONCLUSIVE
        assert "includes_zero" in reason

    def test_p_value_is_not_the_decision_rule(self) -> None:
        # A large positive point estimate with a CI that includes zero is
        # inconclusive even if a p-value would be "close".
        decision, _ = decide_measurement(
            health_status=HEALTH_STATUS_PASS,
            incremental_revenue=2000.0,
            incremental_revenue_ci_lower=-50.0,
            incremental_revenue_ci_upper=4050.0,
            iroas=1.4,
        )
        assert decision == DECISION_INCONCLUSIVE


class TestAlignment:
    def test_both_strong_is_aligned_positive(self) -> None:
        assert (
            classify_alignment(
                health_status=HEALTH_STATUS_PASS,
                roas=1.8,
                iroas=1.4,
                incremental_revenue_ci_lower=200.0,
                incremental_revenue_ci_upper=800.0,
            )
            == ALIGNMENT_ALIGNED_POSITIVE
        )

    def test_high_roas_weak_iroas_is_attribution_stronger(self) -> None:
        assert (
            classify_alignment(
                health_status=HEALTH_STATUS_PASS,
                roas=2.5,
                iroas=0.4,
                incremental_revenue_ci_lower=10.0,
                incremental_revenue_ci_upper=80.0,
            )
            == ALIGNMENT_ATTRIBUTION_STRONGER
        )

    def test_low_roas_strong_iroas_is_incrementality_stronger(self) -> None:
        assert (
            classify_alignment(
                health_status=HEALTH_STATUS_PASS,
                roas=0.6,
                iroas=1.3,
                incremental_revenue_ci_lower=100.0,
                incremental_revenue_ci_upper=400.0,
            )
            == ALIGNMENT_INCREMENTALITY_STRONGER
        )

    def test_crossing_zero_is_inconclusive(self) -> None:
        assert (
            classify_alignment(
                health_status=HEALTH_STATUS_PASS,
                roas=2.0,
                iroas=0.8,
                incremental_revenue_ci_lower=-100.0,
                incremental_revenue_ci_upper=300.0,
            )
            == ALIGNMENT_INCONCLUSIVE
        )

    def test_failed_health_is_not_interpretable(self) -> None:
        assert (
            classify_alignment(
                health_status=HEALTH_STATUS_FAIL,
                roas=3.0,
                iroas=2.0,
                incremental_revenue_ci_lower=500.0,
                incremental_revenue_ci_upper=1500.0,
            )
            == ALIGNMENT_NOT_INTERPRETABLE
        )


def _decision_frames(
    *,
    health_status: str = HEALTH_STATUS_PASS,
    incremental_revenue: float = 4000.0,
    ci_lower: float = 2000.0,
    ci_upper: float = 6000.0,
    spend: float = 2000.0,
    attributed_revenue: float = 5000.0,
    leakage_flag: str = "pass",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    lift = pd.DataFrame(
        {
            "campaign_id": [1],
            "treatment_member_count": [80],
            "control_member_count": [20],
            "treatment_conversion_rate": [0.08],
            "control_conversion_rate": [0.03],
            "absolute_lift": [0.05],
            "absolute_lift_ci_lower": [0.02],
            "absolute_lift_ci_upper": [0.08],
            "absolute_lift_p_value": [0.001],
            "incremental_orders": [10.0],
            "incremental_orders_ci_lower": [4.0],
            "incremental_orders_ci_upper": [16.0],
            "incremental_revenue": [incremental_revenue],
            "incremental_revenue_ci_lower": [ci_lower],
            "incremental_revenue_ci_upper": [ci_upper],
        }
    )
    spend_df = pd.DataFrame(
        {
            "campaign_id": [1],
            "spend_usd": [spend],
            "orders": [40],
            "revenue_usd": [attributed_revenue],
            "roas": [attributed_revenue / spend],
        }
    )
    health = pd.DataFrame(
        {
            "campaign_id": [1],
            "experiment_health_status": [health_status],
            "srm_flag": ["pass"],
            "control_exposure_leakage_flag": [leakage_flag],
            "duplicate_assignment_flag": ["pass"],
            "outcome_completeness_flag": ["pass"],
            "health_reason": ["structural_checks_passed"],
        }
    )
    flags = pd.DataFrame({"campaign_id": [1], "efficiency_flag": ["high_impact"]})
    return lift, spend_df, health, flags


class TestDecisionTable:
    def test_health_failure_blocks_positive_causal_decision(self) -> None:
        lift, spend, health, flags = _decision_frames(health_status=HEALTH_STATUS_FAIL)
        out = build_campaign_decisions(lift, spend, health, flags)
        row = out.iloc[0]
        assert row["measurement_decision"] == DECISION_DO_NOT_INTERPRET
        assert row["attribution_incrementality_alignment"] == ALIGNMENT_NOT_INTERPRETABLE

    def test_attributed_roas_is_not_the_causal_outcome(self) -> None:
        # High attributed ROAS, but incremental-revenue CI includes zero.
        lift, spend, health, flags = _decision_frames(
            incremental_revenue=200.0,
            ci_lower=-400.0,
            ci_upper=800.0,
            spend=100.0,
            attributed_revenue=400.0,
        )
        out = build_campaign_decisions(lift, spend, health, flags)
        row = out.iloc[0]
        assert row["roas"] == pytest.approx(4.0)
        assert row["measurement_decision"] == DECISION_INCONCLUSIVE
        assert row["attribution_incrementality_alignment"] == ALIGNMENT_INCONCLUSIVE

    def test_iroas_uses_incremental_not_attributed_revenue(self) -> None:
        lift, spend, health, flags = _decision_frames(
            incremental_revenue=1000.0,
            ci_lower=500.0,
            ci_upper=1500.0,
            spend=500.0,
            attributed_revenue=4000.0,
        )
        out = build_campaign_decisions(lift, spend, health, flags)
        row = out.iloc[0]
        assert row["iroas"] == pytest.approx(2.0)
        assert row["roas"] == pytest.approx(8.0)
        assert row["iroas"] != pytest.approx(row["roas"])

    def test_preserves_lift_point_estimates(self) -> None:
        lift, spend, health, flags = _decision_frames()
        out = build_campaign_decisions(lift, spend, health, flags)
        assert out.iloc[0]["incremental_revenue"] == pytest.approx(4000.0)
        assert out.iloc[0]["absolute_lift"] == pytest.approx(0.05)
