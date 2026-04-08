"""
Assign treatment/control experiment arms by campaign.

This script reads member and campaign synthetic outputs and produces deterministic per-campaign
randomized holdout assignment using the configured holdout fraction.

Inputs
------
- ``data/synthetic/members.csv``
- ``data/synthetic/campaigns.csv``
- ``configs/simulation_config.yaml``
- ``configs/experiment_config.yaml``

Output
------
- ``data/synthetic/campaign_experiment_assignments.csv``
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import yaml
except ImportError:  # pragma: no cover - optional dependency
    yaml = None

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIM_PATH = REPO_ROOT / "configs" / "simulation_config.yaml"
DEFAULT_EXPERIMENT_PATH = REPO_ROOT / "configs" / "experiment_config.yaml"
DEFAULT_MEMBERS_PATH = REPO_ROOT / "data" / "synthetic" / "members.csv"
DEFAULT_CAMPAIGNS_PATH = REPO_ROOT / "data" / "synthetic" / "campaigns.csv"
OUTPUT_DIR = REPO_ROOT / "data" / "synthetic"
OUTPUT_FILENAME = "campaign_experiment_assignments.csv"

_FALLBACK_SIM = {"simulation": {"random_seed": 42}}
_FALLBACK_EXPERIMENT = {
    "experiment": {
        "design": {"unit": "user", "assignment": "randomized_holdout"},
        "holdout": {"fraction": 0.10, "min_control_members_per_campaign": 15, "max_fraction": 0.45},
    }
}


def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None or not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_simulation_config(path: Path = DEFAULT_SIM_PATH) -> dict[str, Any]:
    data = _load_yaml(path)
    if not data:
        return dict(_FALLBACK_SIM)
    return data


def load_experiment_config(path: Path = DEFAULT_EXPERIMENT_PATH) -> dict[str, Any]:
    data = _load_yaml(path)
    if not data:
        return dict(_FALLBACK_EXPERIMENT)
    return data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assign per-campaign treatment/control experiment arms.")
    parser.add_argument("--simulation-config", type=Path, default=DEFAULT_SIM_PATH)
    parser.add_argument("--experiment-config", type=Path, default=DEFAULT_EXPERIMENT_PATH)
    parser.add_argument("--members-path", type=Path, default=DEFAULT_MEMBERS_PATH)
    parser.add_argument("--campaigns-path", type=Path, default=DEFAULT_CAMPAIGNS_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return parser.parse_args()


def _eligible_members_for_campaign(members: pd.DataFrame, campaign: pd.Series) -> pd.DataFrame:
    """Return members eligible for one campaign using simple retailer/segment/geo targeting rules."""
    mask = members["retailer_id"].eq(int(campaign["retailer_id"]))
    mask &= members["audience_segment_id"].eq(int(campaign["target_audience_segment_id"]))
    mask &= members["primary_geo_id"].eq(int(campaign["target_geo_id"]))
    return members.loc[mask, ["member_id"]].copy()


def assign_by_campaign(
    campaigns: pd.DataFrame,
    members: pd.DataFrame,
    holdout_fraction: float,
    min_control_members_per_campaign: int,
    max_holdout_fraction: float,
    random_seed: int,
) -> pd.DataFrame:
    """
    Create user-level campaign assignments with randomized holdouts.

    One row is created per eligible ``campaign_id`` x ``member_id`` pair and assigned to either
    ``control`` (holdout / not exposed) or ``treatment``.
    """
    if not 0.0 <= holdout_fraction <= 1.0:
        raise ValueError("holdout_fraction must be between 0 and 1 inclusive.")
    if min_control_members_per_campaign < 0:
        raise ValueError("min_control_members_per_campaign must be >= 0.")
    if not 0.0 <= max_holdout_fraction <= 1.0:
        raise ValueError("max_holdout_fraction must be between 0 and 1 inclusive.")
    if max_holdout_fraction < holdout_fraction:
        raise ValueError("max_holdout_fraction must be >= holdout_fraction.")

    required_member_cols = {"member_id", "retailer_id", "audience_segment_id", "primary_geo_id"}
    required_campaign_cols = {
        "campaign_id",
        "retailer_id",
        "target_audience_segment_id",
        "target_geo_id",
    }
    missing_m = required_member_cols - set(members.columns)
    missing_c = required_campaign_cols - set(campaigns.columns)
    if missing_m:
        raise ValueError(f"members missing columns: {sorted(missing_m)}")
    if missing_c:
        raise ValueError(f"campaigns missing columns: {sorted(missing_c)}")

    assignments: list[pd.DataFrame] = []

    for campaign in campaigns.sort_values("campaign_id").itertuples(index=False):
        campaign_dict = campaign._asdict()
        campaign_id = int(campaign_dict["campaign_id"])
        eligible = _eligible_members_for_campaign(members, pd.Series(campaign_dict))
        if eligible.empty:
            continue

        n_eligible = len(eligible)
        min_required_fraction = (
            float(min_control_members_per_campaign) / float(n_eligible) if n_eligible > 0 else holdout_fraction
        )
        effective_holdout_fraction = min(max(holdout_fraction, min_required_fraction), max_holdout_fraction)

        # Campaign-specific deterministic RNG stream.
        stream_seed = int(random_seed + campaign_id * 1009)
        rng = np.random.default_rng(stream_seed)
        draws = rng.random(len(eligible))

        arm = np.where(draws < effective_holdout_fraction, "control", "treatment")

        out = eligible.assign(
            campaign_id=campaign_id,
            experiment_arm=arm,
            assignment_unit="user",
            assignment_method="randomized_holdout",
            holdout_fraction=float(effective_holdout_fraction),
        )
        assignments.append(
            out[
                [
                    "campaign_id",
                    "member_id",
                    "experiment_arm",
                    "assignment_unit",
                    "assignment_method",
                    "holdout_fraction",
                ]
            ]
        )

    if not assignments:
        return pd.DataFrame(
            columns=[
                "campaign_id",
                "member_id",
                "experiment_arm",
                "assignment_unit",
                "assignment_method",
                "holdout_fraction",
            ]
        )

    return pd.concat(assignments, ignore_index=True)


def main() -> None:
    args = _parse_args()

    if not args.members_path.is_file():
        raise FileNotFoundError(f"Missing members file at {args.members_path}. Run scripts/generate_members.py first.")
    if not args.campaigns_path.is_file():
        raise FileNotFoundError(
            f"Missing campaigns file at {args.campaigns_path}. Run scripts/generate_campaigns.py first."
        )

    members = pd.read_csv(args.members_path)
    campaigns = pd.read_csv(args.campaigns_path)

    sim = load_simulation_config(args.simulation_config)
    exp = load_experiment_config(args.experiment_config)

    seed = int(sim.get("simulation", sim).get("random_seed", 42))
    exp_root = exp.get("experiment", exp)
    design = exp_root.get("design", {})
    assignment_method = str(design.get("assignment", "randomized_holdout"))
    unit = str(design.get("unit", "user"))
    if assignment_method != "randomized_holdout" or unit != "user":
        raise ValueError(
            "assign_experiments.py currently supports design.assignment=randomized_holdout and design.unit=user"
        )

    holdout_cfg = exp_root.get("holdout", {})
    holdout_fraction = float(holdout_cfg.get("fraction", 0.10))
    min_control_members_per_campaign = int(holdout_cfg.get("min_control_members_per_campaign", 15))
    max_holdout_fraction = float(holdout_cfg.get("max_fraction", 0.45))

    assignments = assign_by_campaign(
        campaigns=campaigns,
        members=members,
        holdout_fraction=holdout_fraction,
        min_control_members_per_campaign=min_control_members_per_campaign,
        max_holdout_fraction=max_holdout_fraction,
        random_seed=seed,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / OUTPUT_FILENAME
    assignments.to_csv(out_path, index=False)
    print(f"Wrote {len(assignments):,} rows to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
