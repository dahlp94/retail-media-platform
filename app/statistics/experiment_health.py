"""
Experiment-integrity diagnostics for the randomized holdout.

SQL supplies assignment counts, leakage, duplicates, completeness, and
pre-treatment covariate summaries. This module adds:

* chi-square sample-ratio mismatch (SRM) versus the campaign's stored
  holdout fraction
* standardized differences for pre-treatment covariates
* deterministic PASS / WARN / FAIL status

These diagnostics do not change treatment-effect point estimates.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import chi2

HEALTH_STATUS_PASS = "PASS"
HEALTH_STATUS_WARN = "WARN"
HEALTH_STATUS_FAIL = "FAIL"

FLAG_PASS = "pass"
FLAG_WARN = "warn"
FLAG_FAIL = "fail"

HEALTH_ENRICHMENT_COLUMNS = [
    "srm_test_statistic",
    "srm_p_value",
    "srm_flag",
    "control_exposure_leakage_flag",
    "duplicate_assignment_flag",
    "outcome_completeness_flag",
    "preperiod_conversion_smd",
    "signup_tenure_smd",
    "baseline_balance_flag",
    "experiment_health_status",
    "health_reason",
]


def chi_square_srm(
    n_treatment: int,
    n_control: int,
    intended_control_share: float,
) -> dict[str, float]:
    """
    Pearson chi-square goodness-of-fit for two randomized arms.

    Expected counts use the campaign's intended control share (the holdout
    fraction stored on the assignment rows), not a hardcoded 80/20 split.
    """
    empty = {
        "expected_treatment_count": np.nan,
        "expected_control_count": np.nan,
        "srm_test_statistic": np.nan,
        "srm_p_value": np.nan,
    }
    n_t = int(n_treatment)
    n_c = int(n_control)
    n = n_t + n_c
    p_c = float(intended_control_share)
    if n <= 0 or not np.isfinite(p_c) or not 0.0 < p_c < 1.0:
        return empty

    expected_c = n * p_c
    expected_t = n * (1.0 - p_c)
    if expected_t <= 0.0 or expected_c <= 0.0:
        return empty

    statistic = (n_t - expected_t) ** 2 / expected_t + (n_c - expected_c) ** 2 / expected_c
    p_value = float(chi2.sf(statistic, df=1))
    return {
        "expected_treatment_count": expected_t,
        "expected_control_count": expected_c,
        "srm_test_statistic": float(statistic),
        "srm_p_value": p_value,
    }


def classify_srm_flag(
    p_value: float,
    *,
    alpha_fail: float,
    alpha_warn: float,
) -> str:
    if not np.isfinite(p_value):
        return FLAG_FAIL
    if p_value < alpha_fail:
        return FLAG_FAIL
    if p_value < alpha_warn:
        return FLAG_WARN
    return FLAG_PASS


def binary_smd(p_treatment: float, p_control: float) -> float:
    """Cohen-style standardized difference for two proportions."""
    p_t = float(p_treatment)
    p_c = float(p_control)
    if not np.isfinite(p_t) or not np.isfinite(p_c):
        return float("nan")
    pooled = 0.5 * (p_t * (1.0 - p_t) + p_c * (1.0 - p_c))
    if pooled <= 0.0:
        return 0.0 if p_t == p_c else float("nan")
    return (p_t - p_c) / float(np.sqrt(pooled))


def continuous_smd(
    mean_treatment: float,
    mean_control: float,
    sd_treatment: float,
    sd_control: float,
) -> float:
    """Standardized mean difference using the pooled standard deviation."""
    values = (mean_treatment, mean_control, sd_treatment, sd_control)
    if any(not np.isfinite(float(v)) for v in values):
        return float("nan")
    pooled_var = 0.5 * (float(sd_treatment) ** 2 + float(sd_control) ** 2)
    if pooled_var <= 0.0:
        return 0.0 if float(mean_treatment) == float(mean_control) else float("nan")
    return (float(mean_treatment) - float(mean_control)) / float(np.sqrt(pooled_var))


def classify_health_status(
    *,
    srm_flag: str,
    leakage: bool,
    duplicates: bool,
    missing_outcomes: bool,
    balance_warn: bool,
) -> tuple[str, str]:
    """
    Structural integrity first.

    FAIL: leakage, duplicate assignments, missing outcomes, or severe SRM.
    WARN: mild SRM or pre-treatment imbalance worth reviewing.
    PASS: required structural checks succeed.
    """
    fail_reasons: list[str] = []
    warn_reasons: list[str] = []

    if leakage:
        fail_reasons.append("control_exposure_leakage")
    if duplicates:
        fail_reasons.append("duplicate_assignments")
    if missing_outcomes:
        fail_reasons.append("outcome_reconciliation_failure")
    if srm_flag == FLAG_FAIL:
        fail_reasons.append("severe_srm")
    elif srm_flag == FLAG_WARN:
        warn_reasons.append("mild_srm")
    if balance_warn:
        warn_reasons.append("baseline_imbalance")

    if fail_reasons:
        return HEALTH_STATUS_FAIL, ";".join(fail_reasons)
    if warn_reasons:
        return HEALTH_STATUS_WARN, ";".join(warn_reasons)
    return HEALTH_STATUS_PASS, "structural_checks_passed"


def enrich_health_metrics(
    health_df: pd.DataFrame,
    *,
    srm_alpha_fail: float = 0.001,
    srm_alpha_warn: float = 0.05,
    balance_smd_warn: float = 0.10,
) -> pd.DataFrame:
    """Attach SRM, leakage flags, balance SMDs, and overall health status."""
    out = health_df.copy()
    rows: list[dict[str, Any]] = []

    for _, row in out.iterrows():
        srm = chi_square_srm(
            int(row["treatment_member_count"]),
            int(row["control_member_count"]),
            float(row["intended_control_share"]),
        )
        srm_flag = classify_srm_flag(
            srm["srm_p_value"],
            alpha_fail=srm_alpha_fail,
            alpha_warn=srm_alpha_warn,
        )

        leakage = (
            int(row.get("control_impressions", 0) or 0) > 0
            or int(row.get("control_clicks", 0) or 0) > 0
            or int(row.get("control_members_with_impressions", 0) or 0) > 0
            or int(row.get("control_members_with_clicks", 0) or 0) > 0
        )
        duplicates = (
            int(row.get("duplicate_assignment_pair_count", 0) or 0) > 0
            or int(row.get("extra_assignment_row_count", 0) or 0) > 0
        )
        missing_outcomes = int(row.get("missing_member_outcome_count", 0) or 0) != 0

        pre_smd = binary_smd(
            row.get("treatment_preperiod_conversion_rate", np.nan),
            row.get("control_preperiod_conversion_rate", np.nan),
        )
        tenure_smd = continuous_smd(
            row.get("treatment_mean_signup_tenure_days", np.nan),
            row.get("control_mean_signup_tenure_days", np.nan),
            row.get("treatment_sd_signup_tenure_days", np.nan),
            row.get("control_sd_signup_tenure_days", np.nan),
        )
        balance_warn = any(
            np.isfinite(smd) and abs(smd) > balance_smd_warn
            for smd in (pre_smd, tenure_smd)
        )

        status, reason = classify_health_status(
            srm_flag=srm_flag,
            leakage=leakage,
            duplicates=duplicates,
            missing_outcomes=missing_outcomes,
            balance_warn=balance_warn,
        )
        rows.append(
            {
                "srm_test_statistic": srm["srm_test_statistic"],
                "srm_p_value": srm["srm_p_value"],
                "srm_flag": srm_flag,
                "control_exposure_leakage_flag": FLAG_FAIL if leakage else FLAG_PASS,
                "duplicate_assignment_flag": FLAG_FAIL if duplicates else FLAG_PASS,
                "outcome_completeness_flag": FLAG_FAIL if missing_outcomes else FLAG_PASS,
                "preperiod_conversion_smd": pre_smd,
                "signup_tenure_smd": tenure_smd,
                "baseline_balance_flag": FLAG_WARN if balance_warn else FLAG_PASS,
                "experiment_health_status": status,
                "health_reason": reason,
            }
        )

    enrichment = pd.DataFrame(rows, index=out.index)
    for col in HEALTH_ENRICHMENT_COLUMNS:
        out[col] = enrichment[col]
    return out
