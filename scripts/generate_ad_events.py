"""
Generate synthetic ad exposure event logs (impressions and clicks).

This script produces event-level ad delivery data for downstream attribution and
incrementality analysis. It uses campaign experiment assignments so only members
in the treatment arm are eligible for exposure.

Inputs
------
- ``data/synthetic/campaigns.csv``
- ``data/synthetic/campaign_experiment_assignments.csv``
- ``data/synthetic/members.csv``
- ``configs/simulation_config.yaml``

Output
------
- ``data/synthetic/ad_events.csv``
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
DEFAULT_CAMPAIGNS_PATH = REPO_ROOT / "data" / "synthetic" / "campaigns.csv"
DEFAULT_ASSIGNMENTS_PATH = REPO_ROOT / "data" / "synthetic" / "campaign_experiment_assignments.csv"
DEFAULT_MEMBERS_PATH = REPO_ROOT / "data" / "synthetic" / "members.csv"
OUTPUT_DIR = REPO_ROOT / "data" / "synthetic"
OUTPUT_FILENAME = "ad_events.csv"

_FALLBACK_SIM = {
    "simulation": {
        "random_seed": 42,
        "delivery": {
            "base_impressions_per_campaign_per_day": 5000,
            "impression_volatility": 0.15,
        },
        "engagement": {
            "base_ctr": 0.006,
        },
    }
}

_CHANNEL_CTR_MULTIPLIER = {
    "sponsored_products": 1.6,
    "onsite_display": 1.0,
    "onsite_video": 0.75,
}

_MAX_IMPRESSIONS_PER_MEMBER_PER_DAY = 6


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
    parser = argparse.ArgumentParser(description="Generate synthetic impression/click event logs.")
    parser.add_argument("--simulation-config", type=Path, default=DEFAULT_SIM_PATH)
    parser.add_argument("--campaigns-path", type=Path, default=DEFAULT_CAMPAIGNS_PATH)
    parser.add_argument("--assignments-path", type=Path, default=DEFAULT_ASSIGNMENTS_PATH)
    parser.add_argument("--members-path", type=Path, default=DEFAULT_MEMBERS_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return parser.parse_args()


def _validate_input_columns(
    campaigns: pd.DataFrame,
    assignments: pd.DataFrame,
    members: pd.DataFrame,
) -> None:
    required_campaign_cols = {
        "campaign_id",
        "advertiser_id",
        "retailer_id",
        "channel",
        "pricing_model",
        "bid_price_usd",
        "start_date",
        "end_date",
        "target_audience_segment_id",
    }
    required_assignment_cols = {"campaign_id", "member_id", "experiment_arm"}
    required_member_cols = {"member_id", "audience_segment_id", "signup_date"}

    missing_c = required_campaign_cols - set(campaigns.columns)
    missing_a = required_assignment_cols - set(assignments.columns)
    missing_m = required_member_cols - set(members.columns)

    if missing_c:
        raise ValueError(f"campaigns missing columns: {sorted(missing_c)}")
    if missing_a:
        raise ValueError(f"assignments missing columns: {sorted(missing_a)}")
    if missing_m:
        raise ValueError(f"members missing columns: {sorted(missing_m)}")


def _prepare_treatment_population(
    campaigns: pd.DataFrame,
    assignments: pd.DataFrame,
    members: pd.DataFrame,
) -> pd.DataFrame:
    """Join campaign metadata with treatment-eligible members and member attributes."""
    treatment = assignments.loc[assignments["experiment_arm"].eq("treatment"), ["campaign_id", "member_id"]].copy()
    if treatment.empty:
        return pd.DataFrame(columns=["campaign_id", "member_id"])

    members_use = members.loc[:, ["member_id", "audience_segment_id", "signup_date"]].copy()
    members_use["signup_date"] = pd.to_datetime(members_use["signup_date"], errors="coerce")

    campaigns_use = campaigns.loc[
        :,
        [
            "campaign_id",
            "advertiser_id",
            "retailer_id",
            "channel",
            "pricing_model",
            "bid_price_usd",
            "start_date",
            "end_date",
            "target_audience_segment_id",
        ],
    ].copy()
    campaigns_use["start_date"] = pd.to_datetime(campaigns_use["start_date"], errors="coerce").dt.normalize()
    campaigns_use["end_date"] = pd.to_datetime(campaigns_use["end_date"], errors="coerce").dt.normalize()

    merged = treatment.merge(campaigns_use, on="campaign_id", how="inner").merge(members_use, on="member_id", how="inner")

    # Segment fit should usually be exact due assignment eligibility; keep as an explicit feature.
    merged["is_target_segment"] = merged["audience_segment_id"].eq(merged["target_audience_segment_id"])

    return merged


def _member_engagement_score(population: pd.DataFrame) -> np.ndarray:
    """
    Derive an interpretable member-level engagement score for event simulation.

    The score is based on:
    - audience segment rank (higher segment id => slightly higher engagement)
    - member tenure (older signup => slightly higher engagement)
    """
    seg = population["audience_segment_id"].to_numpy(dtype=float)
    seg_rank = (seg - seg.min()) / max(seg.max() - seg.min(), 1.0)
    seg_factor = 0.85 + 0.30 * seg_rank

    start_dates = pd.to_datetime(population["start_date"])
    tenure_days = (start_dates - pd.to_datetime(population["signup_date"])).dt.days.fillna(0).clip(lower=0).to_numpy(dtype=float)
    tenure_factor = 0.90 + 0.20 * np.minimum(tenure_days / 365.0, 1.0)

    fit_factor = np.where(population["is_target_segment"].to_numpy(dtype=bool), 1.0, 0.75)

    return seg_factor * tenure_factor * fit_factor


def _sample_impressions_for_campaign_day(
    campaign_day_population: pd.DataFrame,
    day: pd.Timestamp,
    base_impressions_per_day: float,
    impression_volatility: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Sample impression events for one campaign-day from treatment-eligible members."""
    n_eligible = len(campaign_day_population)
    if n_eligible == 0:
        return pd.DataFrame()

    volatility_draw = float(rng.normal(loc=1.0, scale=impression_volatility))
    expected = max(base_impressions_per_day * volatility_draw, 1.0)
    raw_n_impressions = int(rng.poisson(lam=expected))

    hard_cap = n_eligible * _MAX_IMPRESSIONS_PER_MEMBER_PER_DAY
    n_impressions = min(max(raw_n_impressions, 1), hard_cap)
    if n_impressions == 0:
        return pd.DataFrame()

    engagement = _member_engagement_score(campaign_day_population)
    probs = engagement / engagement.sum()
    sampled_idx = rng.choice(np.arange(n_eligible), size=n_impressions, replace=True, p=probs)
    sampled = campaign_day_population.iloc[sampled_idx].reset_index(drop=True)

    second_of_day = rng.integers(0, 24 * 60 * 60, size=n_impressions, endpoint=False)
    timestamps = day + pd.to_timedelta(second_of_day, unit="s")

    sampled = sampled.assign(
        timestamp=timestamps,
        event_type="impression",
    )
    return sampled


def _add_click_events(
    impressions: pd.DataFrame,
    base_ctr: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate click events from impression events via a simple probabilistic CTR model."""
    if impressions.empty:
        return impressions.copy()

    channel_mult = impressions["channel"].map(_CHANNEL_CTR_MULTIPLIER).fillna(1.0).to_numpy(dtype=float)
    engagement = _member_engagement_score(impressions)

    ctr = base_ctr * channel_mult * engagement
    ctr = np.clip(ctr, 0.0001, 0.20)

    clicked = rng.random(len(impressions)) < ctr
    click_rows = impressions.loc[clicked].copy()
    if click_rows.empty:
        return impressions.copy()

    # Clicks happen shortly after impressions (same day), with a right-skewed delay.
    click_delay_seconds = np.minimum(rng.exponential(scale=35.0, size=len(click_rows)), 3600).astype(int)
    click_rows["timestamp"] = pd.to_datetime(click_rows["timestamp"]) + pd.to_timedelta(click_delay_seconds, unit="s")
    click_rows["event_type"] = "click"

    return pd.concat([impressions, click_rows], ignore_index=True)


def _assign_event_cost(events: pd.DataFrame) -> pd.Series:
    """Assign per-event media cost using campaign pricing model."""
    bid = events["bid_price_usd"].to_numpy(dtype=float)
    pricing = events["pricing_model"].astype(str).to_numpy()
    event_type = events["event_type"].astype(str).to_numpy()

    is_cpm_impression = (pricing == "CPM") & (event_type == "impression")
    is_cpc_click = (pricing == "CPC") & (event_type == "click")

    cost = np.zeros(len(events), dtype=float)
    cost[is_cpm_impression] = bid[is_cpm_impression] / 1000.0
    cost[is_cpc_click] = bid[is_cpc_click]
    return pd.Series(np.round(cost, 6))


def generate_ad_events_dataframe(
    campaigns: pd.DataFrame,
    assignments: pd.DataFrame,
    members: pd.DataFrame,
    sim: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Build event-level ad logs with treatment-only eligibility and impression->click progression.

    Returns one row per ad event with fields needed for downstream attribution and experiment
    analysis.
    """
    _validate_input_columns(campaigns=campaigns, assignments=assignments, members=members)
    population = _prepare_treatment_population(campaigns=campaigns, assignments=assignments, members=members)
    if population.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "member_id",
                "campaign_id",
                "timestamp",
                "event_type",
                "channel",
                "cost",
                "advertiser_id",
                "retailer_id",
            ]
        )

    sim_root = sim.get("simulation", sim)
    delivery = sim_root.get("delivery", {})
    engagement = sim_root.get("engagement", {})

    base_impressions_per_day = float(delivery.get("base_impressions_per_campaign_per_day", 5000))
    impression_volatility = float(delivery.get("impression_volatility", 0.15))
    base_ctr = float(engagement.get("base_ctr", 0.006))

    events_by_campaign: list[pd.DataFrame] = []
    for campaign_id, campaign_pop in population.groupby("campaign_id", sort=True):
        row0 = campaign_pop.iloc[0]
        start_date = pd.Timestamp(row0["start_date"]).normalize()
        end_date = pd.Timestamp(row0["end_date"]).normalize()
        if pd.isna(start_date) or pd.isna(end_date) or end_date < start_date:
            continue

        # Campaign-specific deterministic stream.
        campaign_seed = int(int(campaign_id) * 10_007 + 17)
        campaign_rng = np.random.default_rng(int(rng.integers(0, 1_000_000_000)) + campaign_seed)

        campaign_days = pd.date_range(start_date, end_date, freq="D")
        for day in campaign_days:
            daily_events = _sample_impressions_for_campaign_day(
                campaign_day_population=campaign_pop,
                day=day,
                base_impressions_per_day=base_impressions_per_day,
                impression_volatility=impression_volatility,
                rng=campaign_rng,
            )
            if daily_events.empty:
                continue
            daily_events = _add_click_events(impressions=daily_events, base_ctr=base_ctr, rng=campaign_rng)
            events_by_campaign.append(daily_events)

    if not events_by_campaign:
        return pd.DataFrame(
            columns=[
                "event_id",
                "member_id",
                "campaign_id",
                "timestamp",
                "event_type",
                "channel",
                "cost",
                "advertiser_id",
                "retailer_id",
            ]
        )

    events = pd.concat(events_by_campaign, ignore_index=True)
    events["cost"] = _assign_event_cost(events)
    events["timestamp"] = pd.to_datetime(events["timestamp"], errors="coerce")
    events = events.dropna(subset=["timestamp"]).sort_values(["timestamp", "campaign_id", "member_id", "event_type"]).reset_index(
        drop=True
    )
    events.insert(0, "event_id", np.arange(1, len(events) + 1, dtype=np.int64))

    return events[
        [
            "event_id",
            "member_id",
            "campaign_id",
            "timestamp",
            "event_type",
            "channel",
            "cost",
            "advertiser_id",
            "retailer_id",
        ]
    ]


def main() -> None:
    args = _parse_args()

    if not args.campaigns_path.is_file():
        raise FileNotFoundError(f"Missing campaigns file at {args.campaigns_path}. Run scripts/generate_campaigns.py first.")
    if not args.assignments_path.is_file():
        raise FileNotFoundError(
            f"Missing assignments file at {args.assignments_path}. Run scripts/assign_experiments.py first."
        )
    if not args.members_path.is_file():
        raise FileNotFoundError(f"Missing members file at {args.members_path}. Run scripts/generate_members.py first.")

    sim = load_simulation_config(args.simulation_config)
    seed = int(sim.get("simulation", sim).get("random_seed", 42))
    rng = np.random.default_rng(seed)

    campaigns = pd.read_csv(args.campaigns_path)
    assignments = pd.read_csv(args.assignments_path)
    members = pd.read_csv(args.members_path)

    events = generate_ad_events_dataframe(
        campaigns=campaigns,
        assignments=assignments,
        members=members,
        sim=sim,
        rng=rng,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / OUTPUT_FILENAME
    events.to_csv(out_path, index=False)
    print(f"Wrote {len(events):,} rows to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
