"""
Generate synthetic purchase transactions for baseline and ad-incremental outcomes.

Baseline orders are drawn for all members (including control holdouts) from a
segment-based daily purchase propensity. Experiment-attributed orders are then
generated at assignment level with a realistic conversion process:

- all assigned users (control + treatment) receive a baseline conversion
  probability based on member/campaign fit
- treatment users receive additional incremental uplift based on ad exposure
  and segment responsiveness
- final probability is baseline for control and baseline+uplift for treatment

Inputs
------
- ``data/synthetic/members.csv``
- ``data/synthetic/campaigns.csv``
- ``data/synthetic/campaign_experiment_assignments.csv``
- ``data/synthetic/ad_events.csv`` (impressions identify exposed treatment users)
- ``configs/simulation_config.yaml``

Output
------
- ``data/synthetic/transactions.csv``
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
DEFAULT_MEMBERS_PATH = REPO_ROOT / "data" / "synthetic" / "members.csv"
DEFAULT_CAMPAIGNS_PATH = REPO_ROOT / "data" / "synthetic" / "campaigns.csv"
DEFAULT_ASSIGNMENTS_PATH = REPO_ROOT / "data" / "synthetic" / "campaign_experiment_assignments.csv"
DEFAULT_AD_EVENTS_PATH = REPO_ROOT / "data" / "synthetic" / "ad_events.csv"
OUTPUT_DIR = REPO_ROOT / "data" / "synthetic"
OUTPUT_FILENAME = "transactions.csv"

_FALLBACK_SIM = {
    "simulation": {
        "random_seed": 42,
        "calendar": {"start_date": "2024-01-01", "end_date": "2024-06-30"},
        "conversion": {
            "baseline_daily_order_rate": 0.00055,
            "incremental_daily_rate_per_exposure_day": 0.0035,
            "segment_baseline_mult": [0.88, 0.94, 1.0, 1.08, 1.18],
            "segment_incremental_mult": [0.55, 0.78, 1.0, 1.32, 1.65],
            "order_value": {"mean_usd": 42.0, "std_usd": 18.0},
        },
    }
}

# Targeting fit: eligible members match campaign segment; keep a small residual for analysis hooks.
_MATCH_QUALITY_ON_TARGET = 1.0
_MATCH_QUALITY_OFF_TARGET = 0.72


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
    parser = argparse.ArgumentParser(description="Generate synthetic purchase transactions CSV.")
    parser.add_argument("--simulation-config", type=Path, default=DEFAULT_SIM_PATH)
    parser.add_argument("--members-path", type=Path, default=DEFAULT_MEMBERS_PATH)
    parser.add_argument("--campaigns-path", type=Path, default=DEFAULT_CAMPAIGNS_PATH)
    parser.add_argument("--assignments-path", type=Path, default=DEFAULT_ASSIGNMENTS_PATH)
    parser.add_argument("--ad-events-path", type=Path, default=DEFAULT_AD_EVENTS_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return parser.parse_args()


def _validate_members(members: pd.DataFrame) -> None:
    required = {"member_id", "retailer_id", "audience_segment_id", "outcome_currency"}
    missing = required - set(members.columns)
    if missing:
        raise ValueError(f"members missing columns: {sorted(missing)}")


def _validate_campaigns(campaigns: pd.DataFrame) -> None:
    required = {"campaign_id", "retailer_id", "target_audience_segment_id"}
    missing = required - set(campaigns.columns)
    if missing:
        raise ValueError(f"campaigns missing columns: {sorted(missing)}")


def _validate_assignments(assignments: pd.DataFrame) -> None:
    required = {"campaign_id", "member_id", "experiment_arm"}
    missing = required - set(assignments.columns)
    if missing:
        raise ValueError(f"assignments missing columns: {sorted(missing)}")


def _validate_ad_events(ad_events: pd.DataFrame) -> None:
    required = {"event_id", "member_id", "campaign_id", "timestamp", "event_type", "retailer_id"}
    missing = required - set(ad_events.columns)
    if missing:
        raise ValueError(f"ad_events missing columns: {sorted(missing)}")


def _calendar_range(sim: dict[str, Any]) -> tuple[pd.Timestamp, pd.Timestamp]:
    sim_root = sim.get("simulation", sim)
    cal = sim_root.get("calendar", {})
    start = pd.Timestamp(cal.get("start_date", "2024-01-01")).normalize()
    end = pd.Timestamp(cal.get("end_date", "2024-06-30")).normalize()
    if end < start:
        raise ValueError("simulation calendar end_date must be on/after start_date.")
    return start, end


def _conversion_params(sim: dict[str, Any]) -> dict[str, Any]:
    sim_root = sim.get("simulation", sim)
    conv = {**_FALLBACK_SIM["simulation"]["conversion"], **sim_root.get("conversion", {})}
    ov = {**_FALLBACK_SIM["simulation"]["conversion"]["order_value"], **conv.get("order_value", {})}
    conv["order_value"] = ov
    return conv


def _segment_arrays(conv: dict[str, Any], n_segments: int) -> tuple[np.ndarray, np.ndarray]:
    """Return (baseline_mult, incremental_mult) length n_segments (1-based segment ids in data)."""
    base_default = list(_FALLBACK_SIM["simulation"]["conversion"]["segment_baseline_mult"])
    inc_default = list(_FALLBACK_SIM["simulation"]["conversion"]["segment_incremental_mult"])
    base = list(conv.get("segment_baseline_mult", base_default))
    inc = list(conv.get("segment_incremental_mult", inc_default))
    while len(base) < n_segments:
        base.append(1.0)
    while len(inc) < n_segments:
        inc.append(1.0)
    return np.asarray(base[:n_segments], dtype=float), np.asarray(inc[:n_segments], dtype=float)


def _sample_order_values(n: int, mean_usd: float, std_usd: float, rng: np.random.Generator) -> np.ndarray:
    """Draw positive order values (USD) with a floor for interpretability."""
    raw = rng.normal(loc=mean_usd, scale=std_usd, size=n)
    return np.maximum(raw, 5.0).round(2)


def _build_baseline_transactions(
    members: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    conv: dict[str, Any],
    baseline_mult: np.ndarray,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    One independent Bernoulli trial per member per calendar day for baseline purchases.

    Propensity scales by audience segment and small per-member noise for heterogeneity.
    """
    n_members = len(members)
    if n_members == 0:
        return pd.DataFrame(
            columns=[
                "transaction_id",
                "member_id",
                "retailer_id",
                "audience_segment_id",
                "order_timestamp",
                "order_value_usd",
                "outcome_currency",
                "purchase_driver",
                "source_campaign_id",
            ]
        )

    member_ids = members["member_id"].to_numpy(dtype=np.int64)
    retailer_ids = members["retailer_id"].to_numpy(dtype=np.int64)
    seg_idx = (members["audience_segment_id"].astype(int).to_numpy() - 1).clip(0, len(baseline_mult) - 1)
    currencies = members["outcome_currency"].astype(str).to_numpy()

    base_rate = float(
        conv.get("baseline_daily_order_rate", _FALLBACK_SIM["simulation"]["conversion"]["baseline_daily_order_rate"])
    )
    p_segment = baseline_mult[seg_idx]
    # Mild cross-member noise (reproducible given rng stream).
    noise = rng.uniform(0.88, 1.12, size=n_members)
    p_day = np.clip(base_rate * p_segment * noise, 1e-7, 0.06)

    rows: list[dict[str, Any]] = []
    for day in calendar:
        hits = rng.random(n_members) < p_day
        if not hits.any():
            continue
        idx = np.flatnonzero(hits)
        n_hits = len(idx)
        seconds = rng.integers(0, 24 * 60 * 60, size=n_hits, endpoint=False)
        ts = day + pd.to_timedelta(seconds, unit="s")
        mean_usd = float(conv["order_value"]["mean_usd"])
        std_usd = float(conv["order_value"]["std_usd"])
        values = _sample_order_values(n_hits, mean_usd, std_usd, rng)
        ts_arr = pd.DatetimeIndex(ts) if not isinstance(ts, pd.DatetimeIndex) else ts
        for k, j in enumerate(idx):
            rows.append(
                {
                    "member_id": int(member_ids[j]),
                    "retailer_id": int(retailer_ids[j]),
                    "audience_segment_id": int(seg_idx[j] + 1),
                    "order_timestamp": ts_arr[k],
                    "order_value_usd": float(values[k]),
                    "outcome_currency": currencies[j],
                    "purchase_driver": "baseline",
                    "source_campaign_id": pd.NA,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "transaction_id",
                "member_id",
                "retailer_id",
                "audience_segment_id",
                "order_timestamp",
                "order_value_usd",
                "outcome_currency",
                "purchase_driver",
                "source_campaign_id",
            ]
        )

    return pd.DataFrame(rows)


def _exposure_day_table(
    ad_events: pd.DataFrame,
    assignments: pd.DataFrame,
    campaigns: pd.DataFrame,
    members: pd.DataFrame,
) -> pd.DataFrame:
    """
    One row per (member, campaign, local date) with impression count and metadata.

    Only treatment arms with at least one impression contribute to incremental eligibility.
    """
    imps = ad_events.loc[ad_events["event_type"].eq("impression"), ["member_id", "campaign_id", "timestamp", "retailer_id"]].copy()
    if imps.empty:
        return pd.DataFrame(
            columns=[
                "member_id",
                "campaign_id",
                "order_date",
                "n_impressions",
                "last_impression_ts",
                "audience_segment_id",
                "target_audience_segment_id",
                "experiment_arm",
            ]
        )

    imps["order_date"] = pd.to_datetime(imps["timestamp"]).dt.normalize()
    treat_keys = assignments.loc[assignments["experiment_arm"].eq("treatment"), ["member_id", "campaign_id"]].drop_duplicates()
    imps = imps.merge(treat_keys, on=["member_id", "campaign_id"], how="inner")
    if imps.empty:
        return pd.DataFrame(
            columns=[
                "member_id",
                "campaign_id",
                "order_date",
                "n_impressions",
                "last_impression_ts",
                "audience_segment_id",
                "target_audience_segment_id",
                "experiment_arm",
            ]
        )

    g = imps.groupby(["member_id", "campaign_id", "order_date"], as_index=False).agg(
        n_impressions=("timestamp", "size"),
        last_impression_ts=("timestamp", "max"),
    )
    g = g.merge(members.loc[:, ["member_id", "audience_segment_id"]], on="member_id", how="left")
    g = g.merge(campaigns.loc[:, ["campaign_id", "target_audience_segment_id"]], on="campaign_id", how="left")
    out = g
    out["experiment_arm"] = "treatment"
    return out


def _build_incremental_transactions(
    assignments: pd.DataFrame,
    campaigns: pd.DataFrame,
    members: pd.DataFrame,
    ad_events: pd.DataFrame,
    conv: dict[str, Any],
    incremental_mult: np.ndarray,
    baseline_mult: np.ndarray,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Simulate campaign-attributed conversions using baseline + treatment uplift.

    Baseline conversion applies to every assignment (control and treatment) and
    reflects member segment fit, targeting alignment, tenure, and light random
    variation. Treatment receives additional uplift that is intentionally modest.
    """
    if assignments.empty:
        return pd.DataFrame(
            columns=[
                "transaction_id",
                "member_id",
                "retailer_id",
                "audience_segment_id",
                "order_timestamp",
                "order_value_usd",
                "outcome_currency",
                "purchase_driver",
                "source_campaign_id",
            ]
        )

    assign = assignments.loc[:, ["campaign_id", "member_id", "experiment_arm"]].copy()
    camp = campaigns.loc[:, ["campaign_id", "retailer_id", "target_audience_segment_id", "start_date", "end_date"]].copy()
    mem = members.loc[:, ["member_id", "audience_segment_id", "signup_date", "outcome_currency"]].copy()
    assign = assign.merge(camp, on="campaign_id", how="inner").merge(mem, on="member_id", how="inner")
    if assign.empty:
        return pd.DataFrame(
            columns=[
                "transaction_id",
                "member_id",
                "retailer_id",
                "audience_segment_id",
                "order_timestamp",
                "order_value_usd",
                "outcome_currency",
                "purchase_driver",
                "source_campaign_id",
            ]
        )

    assign["start_date"] = pd.to_datetime(assign["start_date"]).dt.normalize()
    assign["end_date"] = pd.to_datetime(assign["end_date"]).dt.normalize()
    assign["signup_date"] = pd.to_datetime(assign["signup_date"], errors="coerce").dt.normalize()
    assign = assign.loc[assign["end_date"].ge(assign["start_date"])].copy()
    if assign.empty:
        return pd.DataFrame(
            columns=[
                "transaction_id",
                "member_id",
                "retailer_id",
                "audience_segment_id",
                "order_timestamp",
                "order_value_usd",
                "outcome_currency",
                "purchase_driver",
                "source_campaign_id",
            ]
        )

    # Treatment ad exposure intensity from impression counts by assignment.
    imp_counts = (
        ad_events.loc[ad_events["event_type"].eq("impression"), ["member_id", "campaign_id"]]
        .groupby(["member_id", "campaign_id"], as_index=False)
        .size()
        .rename(columns={"size": "n_impressions"})
    )
    assign = assign.merge(imp_counts, on=["member_id", "campaign_id"], how="left")
    assign["n_impressions"] = assign["n_impressions"].fillna(0).astype(float)

    base_inc = float(conv.get("incremental_daily_rate_per_exposure_day", 0.0035))
    mean_usd = float(conv["order_value"]["mean_usd"])
    std_usd = float(conv["order_value"]["std_usd"])

    seg_idx = (assign["audience_segment_id"].astype(int).to_numpy() - 1).clip(0, len(incremental_mult) - 1)
    seg_uplift = incremental_mult[seg_idx]
    seg_base = baseline_mult[seg_idx]

    on_target = assign["audience_segment_id"].astype(int).eq(assign["target_audience_segment_id"].astype(int)).to_numpy()
    target_fit = np.where(on_target, _MATCH_QUALITY_ON_TARGET, _MATCH_QUALITY_OFF_TARGET)

    tenure_days = (assign["start_date"] - assign["signup_date"]).dt.days.fillna(120.0).clip(lower=0).to_numpy(dtype=float)
    tenure_factor = 0.88 + 0.22 * np.tanh(tenure_days / 365.0)

    # Campaign duration in whole days (inclusive). We scale daily probabilities
    # to campaign-level conversion chance so control is not a one-shot draw.
    campaign_days = (assign["end_date"] - assign["start_date"]).dt.days.add(1).clip(lower=1).to_numpy(dtype=int)

    # Baseline daily conversion probability (applies to both control+treatment).
    baseline_daily = float(
        conv.get("baseline_daily_order_rate", _FALLBACK_SIM["simulation"]["conversion"]["baseline_daily_order_rate"])
    )
    base_noise = rng.uniform(0.88, 1.12, size=len(assign))
    baseline_prob = np.clip(baseline_daily * seg_base * target_fit * tenure_factor * base_noise, 1e-6, 0.03)

    # Convert daily baseline probability to campaign-level conversion probability:
    # P(convert over campaign) = 1 - (1 - p_daily) ^ n_days
    baseline_campaign_prob = 1.0 - np.power(1.0 - baseline_prob, campaign_days)

    # Treatment uplift (causal ad impact): modest incremental increase.
    # Uplift grows with exposure intensity but remains smaller than baseline.
    n_imp = assign["n_impressions"].to_numpy(dtype=float)
    intensity = np.log1p(n_imp) / np.log(6.0)
    intensity = np.clip(intensity, 0.0, 1.6)
    uplift_raw = base_inc * seg_uplift * target_fit * intensity
    uplift_noise = rng.uniform(0.75, 1.25, size=len(assign))
    uplift_prob = np.clip(uplift_raw * uplift_noise, 0.0, 0.03)

    # Apply uplift at daily level for treatment, then convert to campaign-level
    # probability using the same duration scaling.
    treatment_daily_prob = np.clip(baseline_prob + uplift_prob, 1e-6, 0.08)
    treatment_campaign_prob = 1.0 - np.power(1.0 - treatment_daily_prob, campaign_days)

    is_treatment = assign["experiment_arm"].astype(str).eq("treatment").to_numpy()
    final_prob = baseline_campaign_prob.copy()
    final_prob[is_treatment] = treatment_campaign_prob[is_treatment]
    final_prob = np.clip(final_prob, 0.0, 0.35)

    draws = rng.random(len(assign)) < final_prob
    picked = assign.loc[draws].reset_index(drop=True)
    if picked.empty:
        return pd.DataFrame(
            columns=[
                "transaction_id",
                "member_id",
                "retailer_id",
                "audience_segment_id",
                "order_timestamp",
                "order_value_usd",
                "outcome_currency",
                "purchase_driver",
                "source_campaign_id",
            ]
        )

    n = len(picked)
    # Place conversion time inside campaign window with light day-time randomness.
    campaign_days = (picked["end_date"] - picked["start_date"]).dt.days.clip(lower=0).to_numpy(dtype=int)
    day_offset = np.array([rng.integers(0, d + 1) for d in campaign_days], dtype=int)
    sec_offset = rng.integers(0, 24 * 60 * 60, size=n, endpoint=False)
    order_ts = picked["start_date"] + pd.to_timedelta(day_offset, unit="D") + pd.to_timedelta(sec_offset, unit="s")
    values = _sample_order_values(n, mean_usd, std_usd, rng)

    return pd.DataFrame(
        {
            "member_id": picked["member_id"].astype(np.int64),
            "retailer_id": picked["retailer_id"].astype(np.int64),
            "audience_segment_id": picked["audience_segment_id"].astype(np.int64),
            "order_timestamp": order_ts,
            "order_value_usd": values,
            "outcome_currency": picked["outcome_currency"].astype(str),
            "purchase_driver": "experiment_conversion",
            "source_campaign_id": picked["campaign_id"].astype(np.int64),
        }
    )


def generate_transactions_dataframe(
    members: pd.DataFrame,
    campaigns: pd.DataFrame,
    assignments: pd.DataFrame,
    ad_events: pd.DataFrame,
    sim: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Combine baseline and incremental transaction draws into one ordered fact table.

    Control users contribute only to baseline. Treatment users contribute to baseline
    and may contribute incremental rows when impressions exist.
    """
    _validate_members(members)
    _validate_campaigns(campaigns)
    _validate_assignments(assignments)
    if not ad_events.empty:
        _validate_ad_events(ad_events)

    start, end = _calendar_range(sim)
    calendar = pd.date_range(start, end, freq="D")
    conv = _conversion_params(sim)

    n_seg = int(members["audience_segment_id"].max()) if len(members) else 5
    baseline_mult, incremental_mult = _segment_arrays(conv, max(n_seg, 5))

    baseline_df = _build_baseline_transactions(
        members=members,
        calendar=calendar,
        conv=conv,
        baseline_mult=baseline_mult,
        rng=rng,
    )

    incremental_df = _build_incremental_transactions(
        assignments=assignments,
        campaigns=campaigns,
        members=members,
        ad_events=ad_events,
        conv=conv,
        incremental_mult=incremental_mult,
        baseline_mult=baseline_mult,
        rng=rng,
    )

    frames = [baseline_df, incremental_df]
    frames = [f for f in frames if not f.empty]
    if not frames:
        out = pd.DataFrame(
            columns=[
                "transaction_id",
                "member_id",
                "retailer_id",
                "audience_segment_id",
                "order_timestamp",
                "order_value_usd",
                "outcome_currency",
                "purchase_driver",
                "source_campaign_id",
            ]
        )
    else:
        out = pd.concat(frames, ignore_index=True)
        out["order_timestamp"] = pd.to_datetime(out["order_timestamp"])
        out = out.sort_values("order_timestamp", kind="mergesort").reset_index(drop=True)
        out.insert(0, "transaction_id", np.arange(1, len(out) + 1, dtype=np.int64))
        out["source_campaign_id"] = pd.to_numeric(out["source_campaign_id"], errors="coerce").astype("Int64")

    return out


def main() -> None:
    args = _parse_args()

    if not args.members_path.is_file():
        raise FileNotFoundError(f"Missing members file at {args.members_path}. Run scripts/generate_members.py first.")
    if not args.campaigns_path.is_file():
        raise FileNotFoundError(f"Missing campaigns file at {args.campaigns_path}. Run scripts/generate_campaigns.py first.")
    if not args.assignments_path.is_file():
        raise FileNotFoundError(
            f"Missing assignments file at {args.assignments_path}. Run scripts/assign_experiments.py first."
        )
    if not args.ad_events_path.is_file():
        raise FileNotFoundError(
            f"Missing ad events file at {args.ad_events_path}. Run scripts/generate_ad_events.py first."
        )

    sim = load_simulation_config(args.simulation_config)
    seed = int(sim.get("simulation", sim).get("random_seed", 42))
    rng = np.random.default_rng(seed)

    members = pd.read_csv(args.members_path)
    campaigns = pd.read_csv(args.campaigns_path)
    assignments = pd.read_csv(args.assignments_path)
    ad_events = pd.read_csv(args.ad_events_path)

    transactions = generate_transactions_dataframe(
        members=members,
        campaigns=campaigns,
        assignments=assignments,
        ad_events=ad_events,
        sim=sim,
        rng=rng,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / OUTPUT_FILENAME
    transactions.to_csv(out_path, index=False)
    print(f"Wrote {len(transactions):,} rows to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
