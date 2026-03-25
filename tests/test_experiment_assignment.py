"""
Tests for **randomized user holdout** assignment aligned with ``configs/experiment_config.yaml``
(design.unit: user, design.assignment: randomized_holdout, holdout.fraction).

Assignment is defined here as the reference behavior for the synthetic pipeline: stable ordering by
``member_id``, Bernoulli draw per member with the global holdout fraction, reproducible via the
simulation random seed. Downstream generators should match this contract when they persist
assignments to ``data/synthetic/``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC_DIR = REPO_ROOT / "data" / "synthetic"
MEMBERS_PATH = SYNTHETIC_DIR / "members.csv"
SIMULATION_CONFIG = REPO_ROOT / "configs" / "simulation_config.yaml"
EXPERIMENT_CONFIG = REPO_ROOT / "configs" / "experiment_config.yaml"

ARMS = frozenset({"control", "treatment"})


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError:
        return {}
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _pipeline_seed() -> int:
    data = _load_yaml(SIMULATION_CONFIG)
    sim = data.get("simulation", data) if data else {}
    return int(sim.get("random_seed", 42))


def _holdout_fraction() -> float:
    data = _load_yaml(EXPERIMENT_CONFIG)
    exp = data.get("experiment", data) if data else {}
    hold = exp.get("holdout", {})
    return float(hold.get("fraction", 0.10))


def assign_randomized_user_holdout(
    member_ids: np.ndarray | list[int],
    holdout_fraction: float,
    random_seed: int,
) -> pd.DataFrame:
    """
    Assign each member to ``control`` (holdout / not exposed) or ``treatment`` (eligible).

    Members are processed in ascending ``member_id`` order so the draw sequence does not depend on
    CSV row order. ``holdout_fraction`` is the probability of ``control``.
    """
    if not 0.0 <= holdout_fraction <= 1.0:
        raise ValueError("holdout_fraction must be between 0 and 1 inclusive.")
    mids = np.sort(np.unique(np.asarray(member_ids, dtype=np.int64)))
    rng = np.random.default_rng(random_seed)
    uniform = rng.random(len(mids))
    arm = np.where(uniform < holdout_fraction, "control", "treatment")
    return pd.DataFrame({"member_id": mids, "experiment_arm": arm})


def _require_members_csv() -> pd.DataFrame:
    if not MEMBERS_PATH.is_file():
        pytest.skip(f"Missing {MEMBERS_PATH}; run scripts/generate_members.py first.")
    return pd.read_csv(MEMBERS_PATH)


class TestAssignRandomizedUserHoldoutPure:
    """Unit checks on the assignment primitive (no filesystem)."""

    def test_determinism(self) -> None:
        ids = np.array([9, 2, 5, 1], dtype=np.int64)
        a = assign_randomized_user_holdout(ids, 0.2, 999)
        b = assign_randomized_user_holdout(ids, 0.2, 999)
        pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True))

    def test_sorted_member_order_in_output(self) -> None:
        out = assign_randomized_user_holdout([30, 10, 20], 0.5, 1)
        assert out["member_id"].tolist() == [10, 20, 30]

    def test_one_row_per_unique_member(self) -> None:
        out = assign_randomized_user_holdout([1, 1, 2, 2], 0.5, 0)
        assert len(out) == 2
        assert out["member_id"].is_unique

    def test_arms_are_binary_labels(self) -> None:
        out = assign_randomized_user_holdout(np.arange(1, 51), 0.1, 42)
        assert set(out["experiment_arm"].unique()) <= ARMS

    def test_invalid_fraction_raises(self) -> None:
        with pytest.raises(ValueError):
            assign_randomized_user_holdout([1], -0.01, 0)
        with pytest.raises(ValueError):
            assign_randomized_user_holdout([1], 1.01, 0)

    def test_all_control_when_fraction_one(self) -> None:
        out = assign_randomized_user_holdout([1, 2, 3], 1.0, 123)
        assert (out["experiment_arm"] == "control").all()

    def test_all_treatment_when_fraction_zero(self) -> None:
        out = assign_randomized_user_holdout([1, 2, 3], 0.0, 123)
        assert (out["experiment_arm"] == "treatment").all()


class TestAssignmentAgainstMembersCsv:
    """Integration-style checks using ``data/synthetic/members.csv``."""

    def test_covers_all_members_exactly_once(self) -> None:
        members = _require_members_csv()
        seed = _pipeline_seed()
        frac = _holdout_fraction()
        assigned = assign_randomized_user_holdout(members["member_id"].values, frac, seed)
        assert len(assigned) == len(members)
        assert assigned["member_id"].is_unique
        assert set(assigned["member_id"]) == set(members["member_id"])

    def test_holdout_rate_near_config_for_large_population(self) -> None:
        members = _require_members_csv()
        if len(members) < 1000:
            pytest.skip("Need a large member file for stable rate check.")
        frac = _holdout_fraction()
        assigned = assign_randomized_user_holdout(
            members["member_id"].values, frac, _pipeline_seed()
        )
        control_share = (assigned["experiment_arm"] == "control").mean()
        # Binomial SE ~ sqrt(p(1-p)/n); allow generous band for CI machines.
        assert control_share == pytest.approx(frac, abs=0.02)

    def test_different_seeds_produce_different_splits_when_possible(self) -> None:
        members = _require_members_csv()
        if len(members) < 5:
            pytest.skip("Need enough members to compare seeds.")
        frac = _holdout_fraction()
        a = assign_randomized_user_holdout(members["member_id"].values, frac, 111)
        b = assign_randomized_user_holdout(members["member_id"].values, frac, 222)
        # With real data it is virtually certain the arms differ somewhere.
        assert not a["experiment_arm"].equals(b["experiment_arm"])


OPTIONAL_ASSIGNMENT_PATH = SYNTHETIC_DIR / "experiment_assignment.csv"


@pytest.mark.skipif(
    not OPTIONAL_ASSIGNMENT_PATH.is_file(),
    reason="Optional until a generator writes experiment_assignment.csv",
)
def test_persisted_experiment_assignment_matches_members() -> None:
    """If ``experiment_assignment.csv`` exists, it must align one-to-one with ``members.csv``."""
    assignment_df = pd.read_csv(OPTIONAL_ASSIGNMENT_PATH)
    assert {"member_id", "experiment_arm"}.issubset(assignment_df.columns)
    assert assignment_df["member_id"].is_unique
    assert set(assignment_df["experiment_arm"].unique()) <= ARMS
    members = _require_members_csv()
    assert len(assignment_df) == len(members)
    assert set(assignment_df["member_id"]) == set(members["member_id"])
