"""
Generate a synthetic **advertiser** dimension table for the retail media network.

Row count and retailer linkage follow ``configs/simulation_config.yaml`` (``n_advertisers``, ``n_retailers``).
Calendar bounds from the same file anchor ``created_at``. Randomness uses the configured ``random_seed``
so runs are reproducible for audits and CI (see simulation config header comment).

Outputs
-------
``data/synthetic/advertisers.csv`` — one row per advertiser with stable integer ``advertiser_id``.

If `PyYAML` is installed, values are read from ``configs/simulation_config.yaml``; otherwise built-in
defaults match the checked-in file for the keys this script uses.
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
OUTPUT_DIR = REPO_ROOT / "data" / "synthetic"
OUTPUT_FILENAME = "advertisers.csv"

_FALLBACK_SIM = {
    "simulation": {
        "random_seed": 42,
        "calendar": {"start_date": "2024-01-01", "end_date": "2024-06-30"},
        "entities": {"n_retailers": 2, "n_advertisers": 8},
    }
}

# Synthetic vertical labels for portfolio-style demos (not in YAML; stable given seed).
_VERTICAL_CODES = np.array(
    [
        "beverages",
        "snacks",
        "household",
        "personal_care",
        "pantry",
        "frozen",
        "health",
        "baby",
    ],
    dtype=object,
)


def _load_yaml(path: Path) -> dict[str, Any]:
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
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
    return parser.parse_args()


def generate_advertisers_dataframe(sim: dict[str, Any], rng: np.random.Generator) -> pd.DataFrame:
    """
    Build the advertisers dimension: one row per advertiser, balanced across retailers where possible.

    ``advertiser_name`` is deterministic given the seed (shuffled label order) for readable demos.
    ``vertical_code`` cycles through a small taxonomy aligned with CPG-style retail media.
    """
    sim_root = sim.get("simulation", sim)
    entities = sim_root.get("entities", {})
    n_retailers = int(entities.get("n_retailers", 2))
    n_advertisers = int(entities.get("n_advertisers", 8))

    if n_advertisers < 1:
        raise ValueError("n_advertisers must be at least 1.")
    if n_retailers < 1:
        raise ValueError("n_retailers must be at least 1.")

    cal = sim_root.get("calendar", {})
    start = pd.Timestamp(cal.get("start_date", "2024-01-01"))

    advertiser_ids = np.arange(1, n_advertisers + 1, dtype=np.int64)

    # Round-robin assignment so each retailer gets a fair share of advertisers.
    retailer_id = ((advertiser_ids - 1) % n_retailers + 1).astype(np.int64)

    labels = np.array([f"advertiser_{i:03d}" for i in advertiser_ids], dtype=object)
    order = rng.permutation(n_advertisers)
    advertiser_name = labels[order]

    vertical_code = np.take(_VERTICAL_CODES, (advertiser_ids - 1) % len(_VERTICAL_CODES))

    # Created dates: spread in the 180 days before simulation start (inclusive of start).
    window_start = start - pd.Timedelta(days=179)
    span = max(int((start - window_start).days), 0)
    offset = rng.integers(0, span + 1, size=n_advertisers, endpoint=False)
    created_at = window_start + pd.to_timedelta(offset, unit="D")

    return pd.DataFrame(
        {
            "advertiser_id": advertiser_ids,
            "advertiser_name": advertiser_name,
            "retailer_id": retailer_id,
            "vertical_code": vertical_code,
            "created_at": created_at.normalize(),
        }
    )


def main() -> None:
    args = _parse_args()
    sim = load_simulation_config(args.simulation_config)
    sim_root = sim.get("simulation", sim)
    seed = int(sim_root.get("random_seed", 42))
    rng = np.random.default_rng(seed)

    df = generate_advertisers_dataframe(sim, rng)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / OUTPUT_FILENAME
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df):,} rows to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
