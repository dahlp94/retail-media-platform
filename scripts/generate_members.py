"""
Generate a synthetic retail **member** (shopper / analysis unit) dimension table.

Members align with ``docs/experiment_design.md`` (user-level assignment), ``configs/experiment_config.yaml``
(outcome currency, geo design scale), and ``configs/simulation_config.yaml`` (retailer count, audience
segments, calendar bounds, random seed).

Outputs
-------
``data/synthetic/members.csv`` — one row per member with stable integer ``member_id`` and attributes
used by downstream impression and experiment generators.

If `PyYAML` is installed, config values are read from the YAML files under ``configs/``; otherwise
built-in defaults match the checked-in YAMLs for the keys this script uses.
"""

from __future__ import annotations

import argparse
import os
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
OUTPUT_DIR = REPO_ROOT / "data" / "synthetic"
OUTPUT_FILENAME = "members.csv"

# Fallbacks mirror ``configs/simulation_config.yaml`` and ``configs/experiment_config.yaml`` when PyYAML is absent.
_FALLBACK_SIM = {
    "simulation": {
        "random_seed": 42,
        "calendar": {"start_date": "2024-01-01", "end_date": "2024-06-30"},
        "entities": {
            "n_retailers": 2,
            "n_audience_segments": 5,
        },
    }
}
_FALLBACK_EXPERIMENT = {
    "experiment": {
        "outcome": {"currency": "USD"},
        "geo": {"n_treatment_geos": 40, "n_control_geos": 8},
    }
}

DEFAULT_N_MEMBERS = 25_000


def _load_yaml(path: Path) -> dict[str, Any]:
    """Parse a YAML file; return empty dict if missing or if PyYAML is not installed."""
    if yaml is None or not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_simulation_config(path: Path = DEFAULT_SIM_PATH) -> dict[str, Any]:
    """Load simulation config dict, falling back to repository defaults."""
    data = _load_yaml(path)
    if not data:
        return dict(_FALLBACK_SIM)
    return data


def load_experiment_config(path: Path = DEFAULT_EXPERIMENT_PATH) -> dict[str, Any]:
    """Load experiment config dict, falling back to repository defaults."""
    data = _load_yaml(path)
    if not data:
        return dict(_FALLBACK_EXPERIMENT)
    return data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--n-members",
        type=int,
        default=int(os.environ.get("SIMULATION_N_MEMBERS", DEFAULT_N_MEMBERS)),
        help=f"Number of synthetic members (default: {DEFAULT_N_MEMBERS}, or SIMULATION_N_MEMBERS).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Directory for CSV output (default: {OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--simulation-config",
        type=Path,
        default=DEFAULT_SIM_PATH,
        help="Path to simulation_config.yaml.",
    )
    parser.add_argument(
        "--experiment-config",
        type=Path,
        default=DEFAULT_EXPERIMENT_PATH,
        help="Path to experiment_config.yaml.",
    )
    return parser.parse_args()


def generate_members_dataframe(
    n_members: int,
    sim: dict[str, Any],
    experiment: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Build the members dimension as a DataFrame.

    Columns are stable for staging/marts SQL: integer keys, segment and geo IDs, signup before the
    simulation calendar, and outcome currency from the experiment config.
    """
    if n_members < 1:
        raise ValueError("n_members must be at least 1.")

    sim_root = sim.get("simulation", sim)
    exp_root = experiment.get("experiment", experiment)

    entities = sim_root.get("entities", {})
    n_retailers = int(entities.get("n_retailers", 2))
    n_segments = int(entities.get("n_audience_segments", 5))

    cal = sim_root.get("calendar", {})
    start = pd.Timestamp(cal.get("start_date", "2024-01-01"))

    geo = exp_root.get("geo", {})
    n_geo = int(geo.get("n_treatment_geos", 40)) + int(geo.get("n_control_geos", 8))

    currency = str(exp_root.get("outcome", {}).get("currency", "USD"))

    member_ids = np.arange(1, n_members + 1, dtype=np.int64)

    # Weight retailers evenly; segments evenly; geos evenly (supports geo_holdout designs in docs).
    # high is exclusive unless endpoint=True; use [low, high) so IDs are 1..n inclusive.
    retailer_id = rng.integers(1, n_retailers + 1, size=n_members)
    audience_segment_id = rng.integers(1, n_segments + 1, size=n_members)
    primary_geo_id = rng.integers(1, n_geo + 1, size=n_members)

    # Signups spread before the simulation window so members look "established" during the calendar.
    earliest_signup = start - pd.Timedelta(days=365 * 2)
    span_days = int((start - earliest_signup).days)
    offset_days = rng.integers(0, max(span_days, 1), size=n_members, endpoint=False)
    signup_date = earliest_signup + pd.to_timedelta(offset_days, unit="D")

    df = pd.DataFrame(
        {
            "member_id": member_ids,
            "retailer_id": retailer_id.astype(np.int64),
            "audience_segment_id": audience_segment_id.astype(np.int64),
            "primary_geo_id": primary_geo_id.astype(np.int64),
            "signup_date": signup_date.normalize(),
            "outcome_currency": np.full(n_members, currency, dtype=object),
        }
    )
    return df


def main() -> None:
    args = _parse_args()
    sim = load_simulation_config(args.simulation_config)
    experiment = load_experiment_config(args.experiment_config)

    sim_root = sim.get("simulation", sim)
    seed = int(sim_root.get("random_seed", 42))
    rng = np.random.default_rng(seed)

    df = generate_members_dataframe(args.n_members, sim, experiment, rng)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / OUTPUT_FILENAME
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df):,} rows to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
