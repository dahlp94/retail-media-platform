"""
Tests for experiment-health diagnostics.

Fixtures are small and deterministic. These tests do not read production dumps.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest
from scipy.stats import chi2

from app.statistics.experiment_health import (
    HEALTH_STATUS_FAIL,
    HEALTH_STATUS_PASS,
    HEALTH_STATUS_WARN,
    FLAG_FAIL,
    FLAG_PASS,
    FLAG_WARN,
    binary_smd,
    chi_square_srm,
    classify_health_status,
    classify_srm_flag,
    continuous_smd,
    enrich_health_metrics,
)


def _base_health_row(**overrides) -> dict:
    row = {
        "campaign_id": 1,
        "treatment_member_count": 80,
        "control_member_count": 20,
        "assigned_member_count": 100,
        "intended_control_share": 0.20,
        "control_impressions": 0,
        "control_clicks": 0,
        "control_members_with_impressions": 0,
        "control_members_with_clicks": 0,
        "duplicate_assignment_pair_count": 0,
        "extra_assignment_row_count": 0,
        "missing_member_outcome_count": 0,
        "treatment_preperiod_conversion_rate": 0.10,
        "control_preperiod_conversion_rate": 0.10,
        "treatment_mean_signup_tenure_days": 200.0,
        "control_mean_signup_tenure_days": 200.0,
        "treatment_sd_signup_tenure_days": 40.0,
        "control_sd_signup_tenure_days": 40.0,
    }
    row.update(overrides)
    return row


class TestSampleRatioMismatch:
    def test_exact_allocation_has_p_value_one(self) -> None:
        result = chi_square_srm(80, 20, 0.20)
        assert result["expected_treatment_count"] == pytest.approx(80.0)
        assert result["expected_control_count"] == pytest.approx(20.0)
        assert result["srm_test_statistic"] == pytest.approx(0.0)
        assert result["srm_p_value"] == pytest.approx(1.0)
        assert classify_srm_flag(result["srm_p_value"], alpha_fail=0.001, alpha_warn=0.05) == FLAG_PASS

    def test_small_sampling_deviation_does_not_fail(self) -> None:
        result = chi_square_srm(82, 18, 0.20)
        assert result["srm_p_value"] > 0.05
        assert classify_srm_flag(result["srm_p_value"], alpha_fail=0.001, alpha_warn=0.05) == FLAG_PASS

    def test_distorted_allocation_triggers_srm(self) -> None:
        result = chi_square_srm(50, 50, 0.20)
        expected = (50 - 80) ** 2 / 80 + (50 - 20) ** 2 / 20
        assert result["srm_test_statistic"] == pytest.approx(expected)
        assert result["srm_p_value"] == pytest.approx(float(chi2.sf(expected, 1)))
        assert result["srm_p_value"] < 0.001
        assert classify_srm_flag(result["srm_p_value"], alpha_fail=0.001, alpha_warn=0.05) == FLAG_FAIL

    def test_uses_campaign_holdout_not_hardcoded_ratio(self) -> None:
        # 70/30 intended, observed exactly 70/30.
        result = chi_square_srm(70, 30, 0.30)
        assert result["expected_control_count"] == pytest.approx(30.0)
        assert result["srm_p_value"] == pytest.approx(1.0)

    def test_invalid_share_returns_nan(self) -> None:
        result = chi_square_srm(80, 20, 0.0)
        assert math.isnan(result["srm_p_value"])


class TestControlLeakage:
    def test_zero_control_exposure_passes(self) -> None:
        enriched = enrich_health_metrics(pd.DataFrame([_base_health_row()]))
        assert enriched.iloc[0]["control_exposure_leakage_flag"] == FLAG_PASS
        assert enriched.iloc[0]["experiment_health_status"] == HEALTH_STATUS_PASS

    def test_injected_control_impressions_fail(self) -> None:
        enriched = enrich_health_metrics(
            pd.DataFrame([_base_health_row(control_impressions=3, control_members_with_impressions=2)])
        )
        row = enriched.iloc[0]
        assert row["control_exposure_leakage_flag"] == FLAG_FAIL
        assert row["experiment_health_status"] == HEALTH_STATUS_FAIL
        assert "control_exposure_leakage" in row["health_reason"]


class TestOutcomeReconciliation:
    def test_complete_outcomes_pass(self) -> None:
        enriched = enrich_health_metrics(pd.DataFrame([_base_health_row()]))
        assert enriched.iloc[0]["outcome_completeness_flag"] == FLAG_PASS

    def test_missing_assigned_members_fail(self) -> None:
        enriched = enrich_health_metrics(
            pd.DataFrame([_base_health_row(missing_member_outcome_count=4)])
        )
        row = enriched.iloc[0]
        assert row["outcome_completeness_flag"] == FLAG_FAIL
        assert row["experiment_health_status"] == HEALTH_STATUS_FAIL
        assert "outcome_reconciliation_failure" in row["health_reason"]


class TestDuplicateAssignments:
    def test_unique_pairs_pass(self) -> None:
        enriched = enrich_health_metrics(pd.DataFrame([_base_health_row()]))
        assert enriched.iloc[0]["duplicate_assignment_flag"] == FLAG_PASS

    def test_duplicate_campaign_member_fails(self) -> None:
        enriched = enrich_health_metrics(
            pd.DataFrame(
                [_base_health_row(duplicate_assignment_pair_count=1, extra_assignment_row_count=1)]
            )
        )
        row = enriched.iloc[0]
        assert row["duplicate_assignment_flag"] == FLAG_FAIL
        assert row["experiment_health_status"] == HEALTH_STATUS_FAIL


class TestBaselineBalance:
    def test_identical_preperiod_rates_have_zero_smd(self) -> None:
        assert binary_smd(0.12, 0.12) == pytest.approx(0.0)

    def test_large_smd_warns_but_does_not_fail(self) -> None:
        status, reason = classify_health_status(
            srm_flag=FLAG_PASS,
            leakage=False,
            duplicates=False,
            missing_outcomes=False,
            balance_warn=True,
        )
        assert status == HEALTH_STATUS_WARN
        assert reason == "baseline_imbalance"

    def test_continuous_smd_formula(self) -> None:
        smd = continuous_smd(10.0, 8.0, 2.0, 2.0)
        assert smd == pytest.approx(1.0)

    def test_enrichment_warns_on_large_preperiod_smd(self) -> None:
        enriched = enrich_health_metrics(
            pd.DataFrame(
                [
                    _base_health_row(
                        treatment_preperiod_conversion_rate=0.40,
                        control_preperiod_conversion_rate=0.05,
                    )
                ]
            )
        )
        row = enriched.iloc[0]
        assert abs(row["preperiod_conversion_smd"]) > 0.10
        assert row["baseline_balance_flag"] == FLAG_WARN
        assert row["experiment_health_status"] == HEALTH_STATUS_WARN


class TestHealthPrecedence:
    def test_leakage_fails_even_when_srm_is_clean(self) -> None:
        status, reason = classify_health_status(
            srm_flag=FLAG_PASS,
            leakage=True,
            duplicates=False,
            missing_outcomes=False,
            balance_warn=False,
        )
        assert status == HEALTH_STATUS_FAIL
        assert "control_exposure_leakage" in reason
