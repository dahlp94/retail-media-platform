"""
Validate campaign-level treatment/control assignments in ``data/synthetic/``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC_DIR = REPO_ROOT / "data" / "synthetic"
MEMBERS_PATH = SYNTHETIC_DIR / "members.csv"
CAMPAIGNS_PATH = SYNTHETIC_DIR / "campaigns.csv"
ASSIGNMENTS_PATH = SYNTHETIC_DIR / "campaign_experiment_assignments.csv"
EXPERIMENT_CONFIG = REPO_ROOT / "configs" / "experiment_config.yaml"

ARMS = {"control", "treatment"}
REQUIRED_COLUMNS = {
    "campaign_id",
    "member_id",
    "experiment_arm",
    "assignment_unit",
    "assignment_method",
    "holdout_fraction",
}


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError:
        return {}
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _configured_holdout_fraction() -> float:
    data = _load_yaml(EXPERIMENT_CONFIG)
    exp = data.get("experiment", data) if data else {}
    return float(exp.get("holdout", {}).get("fraction", 0.10))


def _require_csv(path: Path) -> None:
    if not path.is_file():
        pytest.skip(f"Missing {path}; generate it under scripts/ first.")


@pytest.fixture(scope="module")
def members_df() -> pd.DataFrame:
    _require_csv(MEMBERS_PATH)
    return pd.read_csv(MEMBERS_PATH)


@pytest.fixture(scope="module")
def campaigns_df() -> pd.DataFrame:
    _require_csv(CAMPAIGNS_PATH)
    return pd.read_csv(CAMPAIGNS_PATH)


@pytest.fixture(scope="module")
def assignments_df() -> pd.DataFrame:
    _require_csv(ASSIGNMENTS_PATH)
    return pd.read_csv(ASSIGNMENTS_PATH)


def test_assignments_schema_and_non_empty(assignments_df: pd.DataFrame) -> None:
    assert len(assignments_df) > 0
    assert REQUIRED_COLUMNS.issubset(assignments_df.columns)


def test_foreign_keys_to_members_and_campaigns(
    assignments_df: pd.DataFrame,
    members_df: pd.DataFrame,
    campaigns_df: pd.DataFrame,
) -> None:
    assert set(assignments_df["member_id"]).issubset(set(members_df["member_id"]))
    assert set(assignments_df["campaign_id"]).issubset(set(campaigns_df["campaign_id"]))


def test_unique_assignment_per_campaign_member_pair(assignments_df: pd.DataFrame) -> None:
    duplicated = assignments_df.duplicated(subset=["campaign_id", "member_id"])
    assert not duplicated.any()


def test_assignment_labels_and_metadata(assignments_df: pd.DataFrame) -> None:
    assert set(assignments_df["experiment_arm"].unique()) <= ARMS
    assert assignments_df["assignment_unit"].eq("user").all()
    assert assignments_df["assignment_method"].eq("randomized_holdout").all()
    assert assignments_df["holdout_fraction"].between(0.0, 1.0).all()


def test_each_campaign_has_single_arm_per_member_no_overlap(assignments_df: pd.DataFrame) -> None:
    # No member should appear as both treatment and control in same campaign.
    arm_count = (
        assignments_df.groupby(["campaign_id", "member_id"])["experiment_arm"]
        .nunique()
        .rename("arm_count")
    )
    assert (arm_count == 1).all()


def test_reasonable_split_per_campaign(assignments_df: pd.DataFrame) -> None:
    configured = _configured_holdout_fraction()
    summary = assignments_df.assign(is_control=assignments_df["experiment_arm"].eq("control")).groupby(
        "campaign_id", as_index=False
    ).agg(n=("member_id", "count"), control_rate=("is_control", "mean"))

    # For very small campaign populations, random variation can be large.
    large = summary[summary["n"] >= 30]
    if large.empty:
        pytest.skip("No campaign has at least 30 assigned members for a stable split check.")

    # Keep this tolerant to avoid brittle failures while still catching obvious issues.
    assert large["control_rate"].between(max(0.0, configured - 0.20), min(1.0, configured + 0.20)).all()


def test_campaign_has_both_arms_when_population_is_large(assignments_df: pd.DataFrame) -> None:
    per_campaign = assignments_df.groupby("campaign_id")
    for _, group in per_campaign:
        if len(group) >= 50:
            assert {"control", "treatment"}.issubset(set(group["experiment_arm"]))
