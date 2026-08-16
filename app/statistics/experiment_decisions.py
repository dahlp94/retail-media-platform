"""
Deterministic campaign measurement decisions.

Combines attributed performance, randomized incrementality, uncertainty,
and experiment health. This is a rule table, not a trained model.

Health failures withhold causal interpretation regardless of the point
estimate. Incremental revenue is the primary causal metric; iROAS is the
primary efficiency metric. Attributed ROAS is a guardrail / comparison
only and is never used as the RCT outcome.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from app.statistics.experiment_health import HEALTH_STATUS_FAIL

DECISION_DO_NOT_INTERPRET = "do_not_interpret"
DECISION_INCREASE_BUDGET = "increase_budget"
DECISION_MAINTAIN = "maintain"
DECISION_MONITOR = "monitor"
DECISION_REDUCE_BUDGET = "reduce_budget"
DECISION_INCONCLUSIVE = "inconclusive"

ALIGNMENT_ALIGNED_POSITIVE = "aligned_positive"
ALIGNMENT_ATTRIBUTION_STRONGER = "attribution_stronger_than_incrementality"
ALIGNMENT_INCREMENTALITY_STRONGER = "incrementality_stronger_than_attribution"
ALIGNMENT_INCONCLUSIVE = "inconclusive"
ALIGNMENT_NOT_INTERPRETABLE = "not_interpretable"

DECISION_COLUMNS = [
    "iroas",
    "iroas_ci_lower",
    "iroas_ci_upper",
    "incremental_revenue_ci_width",
    "attribution_incrementality_alignment",
    "measurement_decision",
    "decision_reason",
]


def compute_iroas(
    incremental_revenue: float,
    spend_usd: float,
    incremental_revenue_ci_lower: float | None = None,
    incremental_revenue_ci_upper: float | None = None,
) -> dict[str, float]:
    """
    iROAS = estimated incremental revenue / observed campaign spend.

    Spend is treated as fixed, so the incremental-revenue interval is
    divided by the same spend. Zero or missing spend returns NaN.
    """
    empty = {
        "iroas": np.nan,
        "iroas_ci_lower": np.nan,
        "iroas_ci_upper": np.nan,
    }
    spend = float(spend_usd) if spend_usd is not None and pd.notna(spend_usd) else np.nan
    if not np.isfinite(spend) or spend <= 0.0:
        return empty
    if incremental_revenue is None or pd.isna(incremental_revenue):
        return empty

    point = float(incremental_revenue) / spend
    lower = (
        float(incremental_revenue_ci_lower) / spend
        if incremental_revenue_ci_lower is not None
        and pd.notna(incremental_revenue_ci_lower)
        else np.nan
    )
    upper = (
        float(incremental_revenue_ci_upper) / spend
        if incremental_revenue_ci_upper is not None
        and pd.notna(incremental_revenue_ci_upper)
        else np.nan
    )
    if np.isfinite(lower) and np.isfinite(upper) and lower > upper:
        lower, upper = upper, lower
    return {
        "iroas": point,
        "iroas_ci_lower": lower,
        "iroas_ci_upper": upper,
    }


def _ci_excludes_zero_positive(lower: float, upper: float) -> bool:
    return np.isfinite(lower) and np.isfinite(upper) and lower > 0.0


def _ci_excludes_zero_negative(lower: float, upper: float) -> bool:
    return np.isfinite(lower) and np.isfinite(upper) and upper < 0.0


def _ci_crosses_zero(lower: float, upper: float) -> bool:
    return np.isfinite(lower) and np.isfinite(upper) and lower <= 0.0 <= upper


def classify_alignment(
    *,
    health_status: str,
    roas: float,
    iroas: float,
    incremental_revenue_ci_lower: float,
    incremental_revenue_ci_upper: float,
) -> str:
    """Compare attributed ROAS with experimental iROAS. Not a causal test."""
    if health_status == HEALTH_STATUS_FAIL:
        return ALIGNMENT_NOT_INTERPRETABLE
    if _ci_crosses_zero(incremental_revenue_ci_lower, incremental_revenue_ci_upper):
        return ALIGNMENT_INCONCLUSIVE
    if not np.isfinite(float(roas) if pd.notna(roas) else np.nan):
        return ALIGNMENT_INCONCLUSIVE
    if not np.isfinite(float(iroas) if pd.notna(iroas) else np.nan):
        return ALIGNMENT_INCONCLUSIVE

    roas_v = float(roas)
    iroas_v = float(iroas)
    if roas_v >= 1.0 and iroas_v >= 1.0:
        return ALIGNMENT_ALIGNED_POSITIVE
    if roas_v >= 1.0 and iroas_v < 1.0:
        return ALIGNMENT_ATTRIBUTION_STRONGER
    if roas_v < 1.0 and iroas_v >= 1.0:
        return ALIGNMENT_INCREMENTALITY_STRONGER
    return ALIGNMENT_INCONCLUSIVE


def decide_measurement(
    *,
    health_status: str,
    incremental_revenue: float,
    incremental_revenue_ci_lower: float,
    incremental_revenue_ci_upper: float,
    iroas: float,
    scale_min_iroas: float = 1.0,
) -> tuple[str, str]:
    """
    Health first, then interval direction, then economic magnitude.

    A p-value is not used as the decision rule.
    """
    if health_status == HEALTH_STATUS_FAIL:
        return (
            DECISION_DO_NOT_INTERPRET,
            "experiment_health_failed; withhold causal interpretation",
        )

    lower = (
        float(incremental_revenue_ci_lower)
        if incremental_revenue_ci_lower is not None
        and pd.notna(incremental_revenue_ci_lower)
        else np.nan
    )
    upper = (
        float(incremental_revenue_ci_upper)
        if incremental_revenue_ci_upper is not None
        and pd.notna(incremental_revenue_ci_upper)
        else np.nan
    )
    iroas_v = float(iroas) if iroas is not None and pd.notna(iroas) else np.nan
    point = (
        float(incremental_revenue)
        if incremental_revenue is not None and pd.notna(incremental_revenue)
        else np.nan
    )

    if not np.isfinite(lower) or not np.isfinite(upper):
        return DECISION_MONITOR, "incremental_revenue_interval_unavailable"

    if _ci_excludes_zero_negative(lower, upper):
        return (
            DECISION_REDUCE_BUDGET,
            "incremental_revenue_ci_entirely_negative",
        )

    if _ci_excludes_zero_positive(lower, upper):
        if np.isfinite(iroas_v) and iroas_v >= scale_min_iroas:
            return (
                DECISION_INCREASE_BUDGET,
                "incremental_revenue_ci_positive_and_iroas_meets_scale_threshold",
            )
        return (
            DECISION_MAINTAIN,
            "incremental_revenue_ci_positive_but_iroas_below_scale_threshold",
        )

    if _ci_crosses_zero(lower, upper):
        if np.isfinite(point) and point > 0.0:
            return (
                DECISION_INCONCLUSIVE,
                "positive_incremental_revenue_point_estimate_but_ci_includes_zero",
            )
        if np.isfinite(point) and point < 0.0:
            return (
                DECISION_INCONCLUSIVE,
                "negative_incremental_revenue_point_estimate_but_ci_includes_zero",
            )
        return DECISION_INCONCLUSIVE, "incremental_revenue_ci_includes_zero"

    return DECISION_MONITOR, "decision_inputs_incomplete"


def build_campaign_decisions(
    lift_df: pd.DataFrame,
    spend_df: pd.DataFrame,
    health_df: pd.DataFrame,
    flags_df: pd.DataFrame | None = None,
    *,
    scale_min_iroas: float = 1.0,
) -> pd.DataFrame:
    """
    Join attribution, incrementality, health, and efficiency into one row
    per campaign. Lift point estimates are copied through unchanged.
    """
    spend = spend_df.rename(
        columns={
            "orders": "attributed_orders",
            "revenue_usd": "attributed_revenue_usd",
        }
    )
    keep_spend = [
        col
        for col in (
            "campaign_id",
            "spend_usd",
            "attributed_orders",
            "attributed_revenue_usd",
            "roas",
        )
        if col in spend.columns
    ]
    if "roas" not in spend.columns and {
        "attributed_revenue_usd",
        "spend_usd",
    }.issubset(spend.columns):
        spend = spend.copy()
        spend["roas"] = spend["attributed_revenue_usd"] / spend["spend_usd"].replace(
            0, np.nan
        )
        keep_spend.append("roas")

    health_keep = [
        col
        for col in (
            "campaign_id",
            "experiment_health_status",
            "srm_flag",
            "control_exposure_leakage_flag",
            "duplicate_assignment_flag",
            "outcome_completeness_flag",
            "baseline_balance_flag",
            "health_reason",
        )
        if col in health_df.columns
    ]

    lift_keep = [
        col
        for col in (
            "campaign_id",
            "treatment_member_count",
            "control_member_count",
            "treatment_conversion_rate",
            "control_conversion_rate",
            "absolute_lift",
            "absolute_lift_ci_lower",
            "absolute_lift_ci_upper",
            "absolute_lift_p_value",
            "incremental_orders",
            "incremental_orders_ci_lower",
            "incremental_orders_ci_upper",
            "incremental_revenue",
            "incremental_revenue_ci_lower",
            "incremental_revenue_ci_upper",
        )
        if col in lift_df.columns
    ]

    out = lift_df[lift_keep].merge(spend[keep_spend], on="campaign_id", how="left")
    out = out.merge(health_df[health_keep], on="campaign_id", how="left")
    if flags_df is not None and "efficiency_flag" in flags_df.columns:
        out = out.merge(
            flags_df[["campaign_id", "efficiency_flag"]],
            on="campaign_id",
            how="left",
        )
    else:
        out["efficiency_flag"] = pd.NA

    rows: list[dict[str, Any]] = []
    for _, row in out.iterrows():
        iroas = compute_iroas(
            row.get("incremental_revenue"),
            row.get("spend_usd"),
            row.get("incremental_revenue_ci_lower"),
            row.get("incremental_revenue_ci_upper"),
        )
        health_status = (
            str(row["experiment_health_status"])
            if pd.notna(row.get("experiment_health_status"))
            else HEALTH_STATUS_FAIL
        )
        decision, reason = decide_measurement(
            health_status=health_status,
            incremental_revenue=row.get("incremental_revenue"),
            incremental_revenue_ci_lower=row.get("incremental_revenue_ci_lower"),
            incremental_revenue_ci_upper=row.get("incremental_revenue_ci_upper"),
            iroas=iroas["iroas"],
            scale_min_iroas=scale_min_iroas,
        )
        alignment = classify_alignment(
            health_status=health_status,
            roas=row.get("roas"),
            iroas=iroas["iroas"],
            incremental_revenue_ci_lower=row.get("incremental_revenue_ci_lower"),
            incremental_revenue_ci_upper=row.get("incremental_revenue_ci_upper"),
        )
        ci_width = (
            float(row["incremental_revenue_ci_upper"])
            - float(row["incremental_revenue_ci_lower"])
            if pd.notna(row.get("incremental_revenue_ci_upper"))
            and pd.notna(row.get("incremental_revenue_ci_lower"))
            else np.nan
        )
        rows.append(
            {
                **iroas,
                "incremental_revenue_ci_width": ci_width,
                "attribution_incrementality_alignment": alignment,
                "measurement_decision": decision,
                "decision_reason": reason,
            }
        )

    extra = pd.DataFrame(rows, index=out.index)
    for col in DECISION_COLUMNS:
        out[col] = extra[col]
    return out.sort_values("campaign_id").reset_index(drop=True)
