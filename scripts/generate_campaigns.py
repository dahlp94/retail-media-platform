"""
Generate synthetic retail media campaigns.

This script uses existing synthetic dimensions and config files to create campaign metadata
with realistic targeting fields, channel mix, budgets, and active date windows.

Inputs
------
- ``data/synthetic/advertisers.csv``
- ``configs/simulation_config.yaml``

Output
------
- ``data/synthetic/campaigns.csv``
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
DEFAULT_ADVERTISERS_PATH = REPO_ROOT / "data" / "synthetic" / "advertisers.csv"
DEFAULT_MEMBERS_PATH = REPO_ROOT / "data" / "synthetic" / "members.csv"
OUTPUT_DIR = REPO_ROOT / "data" / "synthetic"
OUTPUT_FILENAME = "campaigns.csv"

_FALLBACK_SIM = {
    "simulation": {
        "random_seed": 42,
        "calendar": {"start_date": "2024-01-01", "end_date": "2024-06-30"},
        "entities": {
            "n_campaigns": 24,
            "n_audience_segments": 5,
        },
        "pricing": {"cpm_usd": 14.0, "cpc_usd": 0.85},
    }
}

_CHANNELS = np.array(["sponsored_products", "onsite_display", "onsite_video"], dtype=object)


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
    parser = argparse.ArgumentParser(description="Generate synthetic campaign metadata CSV.")
    parser.add_argument(
        "--simulation-config",
        type=Path,
        default=DEFAULT_SIM_PATH,
        help="Path to simulation_config.yaml.",
    )
    parser.add_argument(
        "--advertisers-path",
        type=Path,
        default=DEFAULT_ADVERTISERS_PATH,
        help="Path to advertisers.csv.",
    )
    parser.add_argument(
        "--members-path",
        type=Path,
        default=DEFAULT_MEMBERS_PATH,
        help="Path to members.csv. Used to keep geo targeting within generated member range.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help=f"Directory for CSV output (default: {OUTPUT_DIR}).",
    )
    return parser.parse_args()


def generate_campaigns_dataframe(
    advertisers: pd.DataFrame,
    members: pd.DataFrame,
    sim: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Create a campaign table with deterministic, config-aligned synthetic values.

    Each campaign maps to one advertiser and therefore one retailer, and includes campaign-level
    targeting fields used later by experiment assignment.
    """
    sim_root = sim.get("simulation", sim)
    entities = sim_root.get("entities", {})
    calendar = sim_root.get("calendar", {})
    pricing = sim_root.get("pricing", {})

    n_campaigns = int(entities.get("n_campaigns", 24))
    n_segments = int(entities.get("n_audience_segments", 5))

    if n_campaigns < 1:
        raise ValueError("n_campaigns must be at least 1.")
    if advertisers.empty:
        raise ValueError("advertisers input must be non-empty.")
    if members.empty:
        raise ValueError("members input must be non-empty.")

    start = pd.Timestamp(calendar.get("start_date", "2024-01-01"))
    end = pd.Timestamp(calendar.get("end_date", "2024-06-30"))
    if end < start:
        raise ValueError("simulation calendar end_date must be on/after start_date.")

    cpm_usd = float(pricing.get("cpm_usd", 14.0))
    cpc_usd = float(pricing.get("cpc_usd", 0.85))

    campaign_id = np.arange(1, n_campaigns + 1, dtype=np.int64)

    sampled = advertisers.sample(n=n_campaigns, replace=True, random_state=int(rng.integers(0, 1_000_000_000)))
    advertiser_id = sampled["advertiser_id"].to_numpy(dtype=np.int64)
    retailer_id = sampled["retailer_id"].to_numpy(dtype=np.int64)

    channel = rng.choice(_CHANNELS, size=n_campaigns, replace=True)

    # Targeting fields used by assignment logic.
    target_audience_segment_id = rng.integers(1, n_segments + 1, size=n_campaigns, dtype=np.int64)

    max_geo = int(members["primary_geo_id"].max())
    target_geo_id = rng.integers(1, max_geo + 1, size=n_campaigns, dtype=np.int64)

    total_days = max(int((end - start).days), 1)
    min_len = 14
    max_len = min(56, total_days)
    if max_len < min_len:
        min_len = max_len

    offset = rng.integers(0, total_days - min_len + 1, size=n_campaigns)
    length = rng.integers(min_len, max_len + 1, size=n_campaigns)

    start_date = start + pd.to_timedelta(offset, unit="D")
    end_date = start_date + pd.to_timedelta(length - 1, unit="D")
    end_date = pd.to_datetime(np.minimum(end_date.values.astype("datetime64[ns]"), end.to_datetime64()))

    budget_low = 2_500.0
    budget_high = 40_000.0
    budget_usd = rng.uniform(budget_low, budget_high, size=n_campaigns).round(2)

    active_days = (end_date - start_date).days + 1
    daily_budget_usd = (budget_usd / np.maximum(active_days, 1)).round(2)

    pricing_model = np.where(channel == "sponsored_products", "CPC", "CPM")
    bid_price_usd = np.where(pricing_model == "CPC", cpc_usd, cpm_usd).astype(float)

    campaign_name = np.array([f"campaign_{cid:03d}" for cid in campaign_id], dtype=object)

    return pd.DataFrame(
        {
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "advertiser_id": advertiser_id,
            "retailer_id": retailer_id,
            "channel": channel,
            "pricing_model": pricing_model,
            "bid_price_usd": np.round(bid_price_usd, 4),
            "budget_usd": budget_usd,
            "daily_budget_usd": daily_budget_usd,
            "target_audience_segment_id": target_audience_segment_id,
            "target_geo_id": target_geo_id,
            "start_date": pd.to_datetime(start_date).normalize(),
            "end_date": pd.to_datetime(end_date).normalize(),
        }
    )


def main() -> None:
    args = _parse_args()
    sim = load_simulation_config(args.simulation_config)
    sim_root = sim.get("simulation", sim)
    seed = int(sim_root.get("random_seed", 42))
    rng = np.random.default_rng(seed)

    if not args.advertisers_path.is_file():
        raise FileNotFoundError(
            f"Missing advertisers file at {args.advertisers_path}. Run scripts/generate_advertisers.py first."
        )
    if not args.members_path.is_file():
        raise FileNotFoundError(
            f"Missing members file at {args.members_path}. Run scripts/generate_members.py first."
        )

    advertisers = pd.read_csv(args.advertisers_path)
    members = pd.read_csv(args.members_path)

    campaigns = generate_campaigns_dataframe(advertisers=advertisers, members=members, sim=sim, rng=rng)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / OUTPUT_FILENAME
    campaigns.to_csv(out_path, index=False)
    print(f"Wrote {len(campaigns):,} rows to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
