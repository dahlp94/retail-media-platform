"""
Campaign-level statistical inference for the randomized holdout experiment.

Conversion lift uses the analytic two-sample difference-in-proportions
standard error. Orders and revenue use a member-level nonparametric
bootstrap, stratified by experimental arm within each campaign.

Point estimates are not recomputed here; callers pass the existing
treatment-control differences from ``marts.experiment_lift_metrics``.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import norm

Z_975 = float(norm.ppf(0.975))  # ≈ 1.959964 at the conventional 95% level

_REQUIRED_LIFT_COLS = {
    "campaign_id",
    "treatment_member_count",
    "control_member_count",
    "treatment_converters",
    "control_converters",
    "treatment_conversion_rate",
    "control_conversion_rate",
    "absolute_lift",
    "incremental_orders_per_member",
    "incremental_revenue_per_member",
    "incremental_orders",
    "incremental_revenue",
}

_REQUIRED_MEMBER_COLS = {
    "campaign_id",
    "member_id",
    "experiment_arm",
    "order_count",
    "revenue_usd",
}

UNCERTAINTY_COLUMNS = [
    "absolute_lift_se",
    "absolute_lift_ci_lower",
    "absolute_lift_ci_upper",
    "absolute_lift_z_score",
    "absolute_lift_p_value",
    "incremental_orders_per_member_se",
    "incremental_orders_per_member_ci_lower",
    "incremental_orders_per_member_ci_upper",
    "incremental_orders_se",
    "incremental_orders_ci_lower",
    "incremental_orders_ci_upper",
    "incremental_revenue_per_member_se",
    "incremental_revenue_per_member_ci_lower",
    "incremental_revenue_per_member_ci_upper",
    "incremental_revenue_se",
    "incremental_revenue_ci_lower",
    "incremental_revenue_ci_upper",
]


def _critical_z(confidence_level: float) -> float:
    if not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must be between 0 and 1 exclusive.")
    return float(norm.ppf(1.0 - (1.0 - confidence_level) / 2.0))


def analytic_conversion_lift(
    p_treatment: float,
    n_treatment: int,
    p_control: float,
    n_control: int,
    *,
    confidence_level: float = 0.95,
    estimate: float | None = None,
) -> dict[str, float]:
    """
    Wald inference for the independent two-sample difference in proportions.

    SE = sqrt( p_T(1-p_T)/n_T + p_C(1-p_C)/n_C )

    When ``estimate`` is provided it is used as the interval center and as the
    numerator of the z-statistic, so existing SQL point estimates are preserved.
    """
    n_t = int(n_treatment)
    n_c = int(n_control)
    empty = {
        "absolute_lift_se": np.nan,
        "absolute_lift_ci_lower": np.nan,
        "absolute_lift_ci_upper": np.nan,
        "absolute_lift_z_score": np.nan,
        "absolute_lift_p_value": np.nan,
    }
    if n_t <= 0 or n_c <= 0:
        return empty
    if not np.isfinite(p_treatment) or not np.isfinite(p_control):
        return empty

    tau = float(p_treatment - p_control) if estimate is None else float(estimate)
    se = float(
        np.sqrt(
            p_treatment * (1.0 - p_treatment) / n_t
            + p_control * (1.0 - p_control) / n_c
        )
    )
    z_crit = _critical_z(confidence_level)

    if se == 0.0:
        z_score = 0.0 if tau == 0.0 else np.inf * np.sign(tau)
        p_value = 1.0 if tau == 0.0 else 0.0
        return {
            "absolute_lift_se": 0.0,
            "absolute_lift_ci_lower": tau,
            "absolute_lift_ci_upper": tau,
            "absolute_lift_z_score": float(z_score),
            "absolute_lift_p_value": p_value,
        }

    z_score = tau / se
    p_value = float(2.0 * norm.sf(abs(z_score)))
    return {
        "absolute_lift_se": se,
        "absolute_lift_ci_lower": tau - z_crit * se,
        "absolute_lift_ci_upper": tau + z_crit * se,
        "absolute_lift_z_score": z_score,
        "absolute_lift_p_value": p_value,
    }


def percentile_ci(samples: np.ndarray, confidence_level: float) -> tuple[float, float]:
    """Percentile bootstrap interval: alpha/2 and 1-alpha/2 quantiles."""
    if samples.size == 0 or not np.isfinite(samples).any():
        return (np.nan, np.nan)
    alpha = 1.0 - confidence_level
    lower = float(np.quantile(samples, alpha / 2.0, method="linear"))
    upper = float(np.quantile(samples, 1.0 - alpha / 2.0, method="linear"))
    return lower, upper


def bootstrap_mean_replicates(
    values: np.ndarray,
    n_iterations: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """
    Resample ``values`` with replacement, preserving length, for each replicate.

    Returns an array of length ``n_iterations`` with the mean of each draw.
    The resampling unit is one experimental member (one entry in ``values``).
    """
    n = int(values.size)
    if n_iterations < 1:
        raise ValueError("n_iterations must be >= 1.")
    if n == 0:
        return np.full(n_iterations, np.nan, dtype=float)
    draws = rng.choice(values, size=(n_iterations, n), replace=True)
    return draws.mean(axis=1)


def bootstrap_arm_differences(
    treatment_orders: np.ndarray,
    treatment_revenue: np.ndarray,
    control_orders: np.ndarray,
    control_revenue: np.ndarray,
    *,
    n_treatment: int,
    n_iterations: int,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """
    Stratified member-level bootstrap of per-member and scaled incremental effects.

    Treatment and control arms are resampled independently. Incremental totals
    use the original treatment sample size for every replicate:

        incremental = n_treatment * (mean_T - mean_C)
    """
    if treatment_orders.size != treatment_revenue.size:
        raise ValueError("Treatment orders and revenue must have the same length.")
    if control_orders.size != control_revenue.size:
        raise ValueError("Control orders and revenue must have the same length.")
    if n_iterations < 1:
        raise ValueError("n_iterations must be >= 1.")

    n_t_obs = int(treatment_orders.size)
    n_c_obs = int(control_orders.size)
    if n_t_obs == 0 or n_c_obs == 0:
        nan = np.full(n_iterations, np.nan, dtype=float)
        return {
            "orders_per_member_difference": nan,
            "revenue_per_member_difference": nan,
            "incremental_orders": nan,
            "incremental_revenue": nan,
        }

    # Resample members (indices), then carry both outcomes from the same draw.
    t_idx = rng.integers(0, n_t_obs, size=(n_iterations, n_t_obs))
    c_idx = rng.integers(0, n_c_obs, size=(n_iterations, n_c_obs))
    t_orders_mean = treatment_orders[t_idx].mean(axis=1)
    t_revenue_mean = treatment_revenue[t_idx].mean(axis=1)
    c_orders_mean = control_orders[c_idx].mean(axis=1)
    c_revenue_mean = control_revenue[c_idx].mean(axis=1)

    orders_diff = t_orders_mean - c_orders_mean
    revenue_diff = t_revenue_mean - c_revenue_mean
    n_t = float(n_treatment)

    return {
        "orders_per_member_difference": orders_diff,
        "revenue_per_member_difference": revenue_diff,
        "incremental_orders": n_t * orders_diff,
        "incremental_revenue": n_t * revenue_diff,
    }


def campaign_rng(random_seed: int, campaign_id: int) -> np.random.Generator:
    """
    Deterministic RNG stream for one campaign.

    Derived from the project simulation seed and campaign_id via SeedSequence
    so two runs on unchanged data produce identical intervals.
    """
    ss = np.random.SeedSequence([int(random_seed), int(campaign_id)])
    return np.random.default_rng(ss)


def _arm_arrays(members: pd.DataFrame, arm: str) -> tuple[np.ndarray, np.ndarray]:
    subset = members.loc[members["experiment_arm"].eq(arm)]
    orders = subset["order_count"].to_numpy(dtype=float)
    revenue = subset["revenue_usd"].to_numpy(dtype=float)
    return orders, revenue


def infer_campaign(
    lift_row: pd.Series,
    members: pd.DataFrame,
    *,
    confidence_level: float,
    bootstrap_iterations: int,
    random_seed: int,
) -> dict[str, float]:
    """Compute conversion Wald inference and orders/revenue bootstrap CIs for one campaign."""
    n_t = int(lift_row["treatment_member_count"])
    n_c = int(lift_row["control_member_count"])
    p_t = (
        float(lift_row["treatment_converters"]) / n_t
        if n_t > 0
        else np.nan
    )
    p_c = (
        float(lift_row["control_converters"]) / n_c
        if n_c > 0
        else np.nan
    )

    conversion = analytic_conversion_lift(
        p_t,
        n_t,
        p_c,
        n_c,
        confidence_level=confidence_level,
        estimate=float(lift_row["absolute_lift"])
        if pd.notna(lift_row["absolute_lift"])
        else None,
    )

    empty_boot = {col: np.nan for col in UNCERTAINTY_COLUMNS if col not in conversion}
    if n_t <= 0 or n_c <= 0 or members.empty:
        return {**conversion, **empty_boot}

    t_orders, t_revenue = _arm_arrays(members, "treatment")
    c_orders, c_revenue = _arm_arrays(members, "control")
    if t_orders.size == 0 or c_orders.size == 0:
        return {**conversion, **empty_boot}

    rng = campaign_rng(random_seed, int(lift_row["campaign_id"]))
    replicates = bootstrap_arm_differences(
        t_orders,
        t_revenue,
        c_orders,
        c_revenue,
        n_treatment=n_t,
        n_iterations=bootstrap_iterations,
        rng=rng,
    )

    def _summarize(name: str) -> dict[str, float]:
        samples = replicates[name]
        se = float(np.std(samples, ddof=1)) if samples.size > 1 else 0.0
        lower, upper = percentile_ci(samples, confidence_level)
        prefix = {
            "orders_per_member_difference": "incremental_orders_per_member",
            "incremental_orders": "incremental_orders",
            "revenue_per_member_difference": "incremental_revenue_per_member",
            "incremental_revenue": "incremental_revenue",
        }[name]
        return {
            f"{prefix}_se": se,
            f"{prefix}_ci_lower": lower,
            f"{prefix}_ci_upper": upper,
        }

    boot = {}
    for key in (
        "orders_per_member_difference",
        "incremental_orders",
        "revenue_per_member_difference",
        "incremental_revenue",
    ):
        boot.update(_summarize(key))
    return {**conversion, **boot}


def enrich_lift_metrics(
    lift_df: pd.DataFrame,
    member_outcomes: pd.DataFrame,
    *,
    confidence_level: float = 0.95,
    bootstrap_iterations: int = 1000,
    random_seed: int = 0,
) -> pd.DataFrame:
    """
    Attach uncertainty columns to existing campaign lift estimates.

    Original point-estimate columns are copied through unchanged.
    """
    missing_lift = _REQUIRED_LIFT_COLS - set(lift_df.columns)
    if missing_lift:
        raise ValueError(f"lift_df missing columns: {sorted(missing_lift)}")
    missing_members = _REQUIRED_MEMBER_COLS - set(member_outcomes.columns)
    if missing_members:
        raise ValueError(f"member_outcomes missing columns: {sorted(missing_members)}")
    if bootstrap_iterations < 1:
        raise ValueError("bootstrap_iterations must be >= 1.")
    _critical_z(confidence_level)

    out = lift_df.copy()
    member_outcomes = member_outcomes.copy()
    member_outcomes["experiment_arm"] = member_outcomes["experiment_arm"].astype(str)

    rows: list[dict[str, Any]] = []
    grouped = member_outcomes.groupby("campaign_id", sort=False)
    for _, lift_row in out.iterrows():
        campaign_id = lift_row["campaign_id"]
        try:
            members = grouped.get_group(campaign_id)
        except KeyError:
            members = member_outcomes.iloc[0:0]
        rows.append(
            infer_campaign(
                lift_row,
                members,
                confidence_level=confidence_level,
                bootstrap_iterations=bootstrap_iterations,
                random_seed=random_seed,
            )
        )

    uncertainty = pd.DataFrame(rows, index=out.index)
    for col in UNCERTAINTY_COLUMNS:
        out[col] = uncertainty[col]
    return out
