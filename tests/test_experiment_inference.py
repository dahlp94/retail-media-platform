"""
Tests for campaign-level conversion Wald inference and member-level bootstrap.

Statistical behavior is checked on small deterministic fixtures, not production dumps.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest
from scipy.stats import norm

from app.statistics.experiment_inference import (
    UNCERTAINTY_COLUMNS,
    analytic_conversion_lift,
    bootstrap_arm_differences,
    bootstrap_mean_replicates,
    campaign_rng,
    enrich_lift_metrics,
    percentile_ci,
)


def _wald_se(p_t: float, n_t: int, p_c: float, n_c: int) -> float:
    return math.sqrt(p_t * (1.0 - p_t) / n_t + p_c * (1.0 - p_c) / n_c)


class TestAnalyticConversionLift:
    def test_equal_conversion_rates(self) -> None:
        p_t, p_c, n_t, n_c = 0.5, 0.5, 100, 100
        result = analytic_conversion_lift(p_t, n_t, p_c, n_c)
        se = _wald_se(p_t, n_t, p_c, n_c)
        z_crit = float(norm.ppf(0.975))

        assert result["absolute_lift_se"] == pytest.approx(se)
        assert result["absolute_lift_z_score"] == pytest.approx(0.0)
        assert result["absolute_lift_p_value"] == pytest.approx(1.0)
        assert result["absolute_lift_ci_lower"] == pytest.approx(-z_crit * se)
        assert result["absolute_lift_ci_upper"] == pytest.approx(z_crit * se)

    def test_treatment_conversion_greater_than_control(self) -> None:
        p_t, p_c, n_t, n_c = 0.6, 0.4, 100, 100
        tau = 0.2
        se = _wald_se(p_t, n_t, p_c, n_c)
        z = tau / se
        p_value = 2.0 * float(norm.sf(abs(z)))
        z_crit = float(norm.ppf(0.975))

        result = analytic_conversion_lift(p_t, n_t, p_c, n_c)
        assert result["absolute_lift_se"] == pytest.approx(se)
        assert result["absolute_lift_z_score"] == pytest.approx(z)
        assert result["absolute_lift_p_value"] == pytest.approx(p_value)
        assert result["absolute_lift_ci_lower"] == pytest.approx(tau - z_crit * se)
        assert result["absolute_lift_ci_upper"] == pytest.approx(tau + z_crit * se)
        assert result["absolute_lift_p_value"] < 0.05

    def test_treatment_conversion_less_than_control(self) -> None:
        p_t, p_c, n_t, n_c = 0.4, 0.6, 100, 100
        tau = -0.2
        se = _wald_se(p_t, n_t, p_c, n_c)
        result = analytic_conversion_lift(p_t, n_t, p_c, n_c)
        assert result["absolute_lift_z_score"] == pytest.approx(tau / se)
        assert result["absolute_lift_z_score"] < 0
        assert result["absolute_lift_ci_upper"] < 0

    def test_zero_converters_in_one_arm(self) -> None:
        p_t, p_c, n_t, n_c = 0.0, 0.1, 100, 100
        se = _wald_se(p_t, n_t, p_c, n_c)
        tau = -0.1
        result = analytic_conversion_lift(p_t, n_t, p_c, n_c)
        assert se > 0
        assert result["absolute_lift_se"] == pytest.approx(se)
        assert result["absolute_lift_z_score"] == pytest.approx(tau / se)
        assert 0.0 < result["absolute_lift_p_value"] < 1.0

    def test_very_small_conversion_rates(self) -> None:
        p_t, p_c, n_t, n_c = 0.0001, 0.0002, 10_000, 10_000
        se = _wald_se(p_t, n_t, p_c, n_c)
        result = analytic_conversion_lift(p_t, n_t, p_c, n_c)
        assert result["absolute_lift_se"] == pytest.approx(se)
        assert result["absolute_lift_se"] > 0
        assert result["absolute_lift_ci_lower"] < (p_t - p_c) < result["absolute_lift_ci_upper"]

    def test_ci_contains_estimate(self) -> None:
        result = analytic_conversion_lift(0.12, 500, 0.08, 500)
        estimate = 0.04
        assert result["absolute_lift_ci_lower"] <= estimate <= result["absolute_lift_ci_upper"]

    def test_preserves_supplied_sql_estimate_as_interval_center(self) -> None:
        sql_estimate = 0.02821233
        result = analytic_conversion_lift(
            0.0557,
            2000,
            0.0275,
            500,
            estimate=sql_estimate,
        )
        z_crit = float(norm.ppf(0.975))
        midpoint = 0.5 * (
            result["absolute_lift_ci_lower"] + result["absolute_lift_ci_upper"]
        )
        assert midpoint == pytest.approx(sql_estimate)
        width = result["absolute_lift_ci_upper"] - result["absolute_lift_ci_lower"]
        assert width == pytest.approx(2.0 * z_crit * result["absolute_lift_se"])

    def test_zero_variance_both_arms_no_converters(self) -> None:
        result = analytic_conversion_lift(0.0, 50, 0.0, 50)
        assert result["absolute_lift_se"] == 0.0
        assert result["absolute_lift_ci_lower"] == 0.0
        assert result["absolute_lift_ci_upper"] == 0.0
        assert result["absolute_lift_z_score"] == 0.0
        assert result["absolute_lift_p_value"] == 1.0

    def test_zero_variance_opposite_certain_rates(self) -> None:
        result = analytic_conversion_lift(1.0, 20, 0.0, 20)
        assert result["absolute_lift_se"] == 0.0
        assert result["absolute_lift_ci_lower"] == 1.0
        assert result["absolute_lift_ci_upper"] == 1.0
        assert math.isinf(result["absolute_lift_z_score"])
        assert result["absolute_lift_p_value"] == 0.0

    def test_empty_arm_returns_nan(self) -> None:
        result = analytic_conversion_lift(0.5, 0, 0.4, 10)
        assert math.isnan(result["absolute_lift_se"])
        assert math.isnan(result["absolute_lift_p_value"])

    def test_invalid_confidence_level_raises(self) -> None:
        with pytest.raises(ValueError, match="confidence_level"):
            analytic_conversion_lift(0.5, 10, 0.4, 10, confidence_level=1.0)

    def test_z_975_matches_conventional_constant(self) -> None:
        result = analytic_conversion_lift(0.5, 100, 0.4, 100)
        implied_z = (result["absolute_lift_ci_upper"] - 0.1) / result["absolute_lift_se"]
        assert implied_z == pytest.approx(1.959964, rel=1e-6)


class TestBootstrap:
    def test_identical_seed_identical_results(self) -> None:
        values = np.array([0.0, 1.0, 2.0, 0.0, 4.0])
        a = bootstrap_mean_replicates(values, 25, np.random.default_rng(7))
        b = bootstrap_mean_replicates(values, 25, np.random.default_rng(7))
        np.testing.assert_array_equal(a, b)

    def test_campaign_rng_is_deterministic(self) -> None:
        a = campaign_rng(0, 9).random(8)
        b = campaign_rng(0, 9).random(8)
        np.testing.assert_array_equal(a, b)
        c = campaign_rng(0, 10).random(8)
        assert not np.array_equal(a, c)

    def test_all_zero_outcomes_zero_estimate_and_zero_width_ci(self) -> None:
        zeros_t = np.zeros(6)
        zeros_c = np.zeros(4)
        rng = np.random.default_rng(1)
        reps = bootstrap_arm_differences(
            zeros_t,
            zeros_t,
            zeros_c,
            zeros_c,
            n_treatment=6,
            n_iterations=40,
            rng=rng,
        )
        for name in (
            "orders_per_member_difference",
            "revenue_per_member_difference",
            "incremental_orders",
            "incremental_revenue",
        ):
            assert np.allclose(reps[name], 0.0)
            lower, upper = percentile_ci(reps[name], 0.95)
            assert lower == 0.0
            assert upper == 0.0

    def test_constant_arm_outcomes(self) -> None:
        treatment = np.full(5, 3.0)
        control = np.full(5, 1.0)
        rng = np.random.default_rng(2)
        reps = bootstrap_arm_differences(
            treatment,
            treatment * 10.0,
            control,
            control * 10.0,
            n_treatment=5,
            n_iterations=30,
            rng=rng,
        )
        assert np.allclose(reps["orders_per_member_difference"], 2.0)
        assert np.allclose(reps["incremental_orders"], 5.0 * 2.0)
        assert np.allclose(reps["revenue_per_member_difference"], 20.0)
        lower, upper = percentile_ci(reps["incremental_orders"], 0.95)
        assert lower == pytest.approx(10.0)
        assert upper == pytest.approx(10.0)

    def test_arms_resampled_independently(self) -> None:
        treatment = np.array([10.0, 20.0, 30.0])
        control = np.array([1.0, 2.0])
        t_means = bootstrap_mean_replicates(treatment, 50, np.random.default_rng(3))
        c_means = bootstrap_mean_replicates(control, 50, np.random.default_rng(4))
        assert t_means.min() >= 10.0
        assert t_means.max() <= 30.0
        assert c_means.min() >= 1.0
        assert c_means.max() <= 2.0
        unique_c = np.unique(np.round(c_means, 10))
        assert set(unique_c).issubset({1.0, 1.5, 2.0})

    def test_original_arm_sample_sizes_preserved(self) -> None:
        values = np.array([0.0, 1.0, 4.0, 9.0])
        rng = np.random.default_rng(5)
        n_iterations = 12
        n = values.size
        draws = rng.choice(values, size=(n_iterations, n), replace=True)
        assert draws.shape == (n_iterations, n)
        means = bootstrap_mean_replicates(values, n_iterations, np.random.default_rng(5))
        np.testing.assert_allclose(means, draws.mean(axis=1))

    def test_non_purchasers_are_retained_as_zeros(self) -> None:
        values = np.array([0.0, 0.0, 5.0])
        means = bootstrap_mean_replicates(values, 200, np.random.default_rng(6))
        assert means.mean() == pytest.approx(5.0 / 3.0, rel=0.15)
        assert means.max() <= 5.0
        assert means.min() >= 0.0

    def test_member_outcomes_resampled_jointly(self) -> None:
        t_orders = np.array([0.0, 1.0, 2.0])
        t_rev = t_orders * 10.0
        c_orders = np.array([0.0, 1.0])
        c_rev = c_orders * 10.0
        reps = bootstrap_arm_differences(
            t_orders,
            t_rev,
            c_orders,
            c_rev,
            n_treatment=3,
            n_iterations=40,
            rng=np.random.default_rng(11),
        )
        np.testing.assert_allclose(
            reps["revenue_per_member_difference"],
            10.0 * reps["orders_per_member_difference"],
        )

    def test_incremental_total_is_n_treatment_times_per_member_effect(self) -> None:
        t_orders = np.array([0.0, 1.0, 2.0, 0.0])
        t_rev = np.array([0.0, 10.0, 20.0, 0.0])
        c_orders = np.array([0.0, 1.0])
        c_rev = np.array([0.0, 8.0])
        n_t = 4
        reps = bootstrap_arm_differences(
            t_orders,
            t_rev,
            c_orders,
            c_rev,
            n_treatment=n_t,
            n_iterations=35,
            rng=np.random.default_rng(8),
        )
        np.testing.assert_allclose(
            reps["incremental_orders"],
            n_t * reps["orders_per_member_difference"],
        )
        np.testing.assert_allclose(
            reps["incremental_revenue"],
            n_t * reps["revenue_per_member_difference"],
        )

    def test_percentile_ci_ordering_on_symmetric_fixture(self) -> None:
        values = np.arange(20.0)
        means = bootstrap_mean_replicates(values, 400, np.random.default_rng(9))
        point = float(values.mean())
        lower, upper = percentile_ci(means, 0.95)
        assert lower <= point <= upper

    def test_empty_arm_replicates_are_nan(self) -> None:
        means = bootstrap_mean_replicates(np.array([]), 10, np.random.default_rng(1))
        assert means.shape == (10,)
        assert np.isnan(means).all()


def _lift_and_members_fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Two treatment / two control members; one purchaser per arm."""
    members = pd.DataFrame(
        {
            "campaign_id": [1, 1, 1, 1],
            "member_id": [1, 2, 3, 4],
            "experiment_arm": ["treatment", "treatment", "control", "control"],
            "order_count": [2, 0, 1, 0],
            "revenue_usd": [40.0, 0.0, 15.0, 0.0],
            "is_converter": [1, 0, 1, 0],
        }
    )
    lift = pd.DataFrame(
        {
            "campaign_id": [1],
            "treatment_member_count": [2],
            "control_member_count": [2],
            "treatment_converters": [1],
            "control_converters": [1],
            "treatment_conversion_rate": [0.5],
            "control_conversion_rate": [0.5],
            "absolute_lift": [0.0],
            "incremental_orders_per_member": [0.5],
            "incremental_revenue_per_member": [12.5],
            "incremental_orders": [1.0],
            "incremental_revenue": [25.0],
        }
    )
    return lift, members


class TestEnrichLiftMetrics:
    def test_does_not_change_point_estimates(self) -> None:
        lift, members = _lift_and_members_fixture()
        original = lift.copy()
        enriched = enrich_lift_metrics(
            lift,
            members,
            confidence_level=0.95,
            bootstrap_iterations=20,
            random_seed=0,
        )
        for col in original.columns:
            assert enriched[col].tolist() == original[col].tolist()

    def test_adds_uncertainty_columns(self) -> None:
        lift, members = _lift_and_members_fixture()
        enriched = enrich_lift_metrics(
            lift,
            members,
            bootstrap_iterations=20,
            random_seed=0,
        )
        for col in UNCERTAINTY_COLUMNS:
            assert col in enriched.columns

    def test_enrichment_is_reproducible(self) -> None:
        lift, members = _lift_and_members_fixture()
        a = enrich_lift_metrics(lift, members, bootstrap_iterations=30, random_seed=0)
        b = enrich_lift_metrics(lift, members, bootstrap_iterations=30, random_seed=0)
        pd.testing.assert_frame_equal(a, b)

    def test_conversion_ci_contains_zero_when_rates_equal(self) -> None:
        lift, members = _lift_and_members_fixture()
        enriched = enrich_lift_metrics(
            lift,
            members,
            bootstrap_iterations=20,
            random_seed=0,
        )
        row = enriched.iloc[0]
        assert row["absolute_lift_ci_lower"] <= 0.0 <= row["absolute_lift_ci_upper"]
        assert row["absolute_lift_p_value"] == pytest.approx(1.0)
