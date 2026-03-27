"""
Validate generated CSV schemas and key constraints in ``data/synthetic/``.

These tests are intentionally simple and focus on practical data contracts used by
downstream SQL and analytics layers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC_DIR = REPO_ROOT / "data" / "synthetic"
SIMULATION_CONFIG = REPO_ROOT / "configs" / "simulation_config.yaml"
EXPERIMENT_CONFIG = REPO_ROOT / "configs" / "experiment_config.yaml"

MEMBERS_PATH = SYNTHETIC_DIR / "members.csv"
ADVERTISERS_PATH = SYNTHETIC_DIR / "advertisers.csv"
CAMPAIGNS_PATH = SYNTHETIC_DIR / "campaigns.csv"
ASSIGNMENTS_PATH = SYNTHETIC_DIR / "campaign_experiment_assignments.csv"

_DEFAULT_ENTITIES = {
    "n_retailers": 2,
    "n_advertisers": 8,
    "n_campaigns": 24,
    "n_audience_segments": 5,
}
_DEFAULT_GEO = {"n_treatment_geos": 40, "n_control_geos": 8}
_DEFAULT_CAL_START = "2024-01-01"
_DEFAULT_CAL_END = "2024-06-30"


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError:
        return {}
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _simulation_bounds() -> dict[str, Any]:
    sim_data = _load_yaml(SIMULATION_CONFIG)
    sim = sim_data.get("simulation", sim_data) if sim_data else {}
    entities = {**_DEFAULT_ENTITIES, **sim.get("entities", {})}
    calendar = sim.get("calendar", {})
    return {
        "entities": entities,
        "calendar_start": pd.Timestamp(calendar.get("start_date", _DEFAULT_CAL_START)),
        "calendar_end": pd.Timestamp(calendar.get("end_date", _DEFAULT_CAL_END)),
    }


def _geo_count() -> int:
    exp_data = _load_yaml(EXPERIMENT_CONFIG)
    exp = exp_data.get("experiment", exp_data) if exp_data else {}
    geo = {**_DEFAULT_GEO, **exp.get("geo", {})}
    return int(geo["n_treatment_geos"]) + int(geo["n_control_geos"])


def _require_csv(path: Path) -> None:
    if not path.is_file():
        pytest.skip(f"Missing {path}; generate it under scripts/ first.")


@pytest.fixture(scope="module")
def members_df() -> pd.DataFrame:
    _require_csv(MEMBERS_PATH)
    return pd.read_csv(MEMBERS_PATH)


@pytest.fixture(scope="module")
def advertisers_df() -> pd.DataFrame:
    _require_csv(ADVERTISERS_PATH)
    return pd.read_csv(ADVERTISERS_PATH)


@pytest.fixture(scope="module")
def campaigns_df() -> pd.DataFrame:
    _require_csv(CAMPAIGNS_PATH)
    return pd.read_csv(CAMPAIGNS_PATH)


@pytest.fixture(scope="module")
def assignments_df() -> pd.DataFrame:
    _require_csv(ASSIGNMENTS_PATH)
    return pd.read_csv(ASSIGNMENTS_PATH)


def test_all_generated_datasets_exist_and_non_empty(
    members_df: pd.DataFrame,
    advertisers_df: pd.DataFrame,
    campaigns_df: pd.DataFrame,
    assignments_df: pd.DataFrame,
) -> None:
    assert len(members_df) > 0
    assert len(advertisers_df) > 0
    assert len(campaigns_df) > 0
    assert len(assignments_df) > 0


class TestMembersSchema:
    REQUIRED_COLUMNS = {
        "member_id",
        "retailer_id",
        "audience_segment_id",
        "primary_geo_id",
        "signup_date",
        "outcome_currency",
    }

    def test_schema(self, members_df: pd.DataFrame) -> None:
        assert self.REQUIRED_COLUMNS.issubset(members_df.columns)

    def test_key_and_ranges(self, members_df: pd.DataFrame) -> None:
        bounds = _simulation_bounds()
        n_retailers = int(bounds["entities"]["n_retailers"])
        n_segments = int(bounds["entities"]["n_audience_segments"])
        n_geo = _geo_count()

        assert members_df["member_id"].is_unique
        assert members_df["retailer_id"].between(1, n_retailers).all()
        assert members_df["audience_segment_id"].between(1, n_segments).all()
        assert members_df["primary_geo_id"].between(1, n_geo).all()

    def test_dates_and_currency(self, members_df: pd.DataFrame) -> None:
        cal_start = _simulation_bounds()["calendar_start"]
        signup_date = pd.to_datetime(members_df["signup_date"])
        assert signup_date.notna().all()
        assert (signup_date < cal_start).all()
        assert members_df["outcome_currency"].astype(str).str.strip().ne("").all()


class TestAdvertisersSchema:
    REQUIRED_COLUMNS = {
        "advertiser_id",
        "advertiser_name",
        "retailer_id",
        "vertical_code",
        "created_at",
    }

    def test_schema(self, advertisers_df: pd.DataFrame) -> None:
        assert self.REQUIRED_COLUMNS.issubset(advertisers_df.columns)

    def test_keys_and_ranges(self, advertisers_df: pd.DataFrame) -> None:
        n_retailers = int(_simulation_bounds()["entities"]["n_retailers"])
        assert advertisers_df["advertiser_id"].is_unique
        assert advertisers_df["advertiser_name"].astype(str).is_unique
        assert advertisers_df["retailer_id"].between(1, n_retailers).all()
        assert advertisers_df["vertical_code"].astype(str).str.strip().ne("").all()

    def test_row_count_matches_config(self, advertisers_df: pd.DataFrame) -> None:
        expected = int(_simulation_bounds()["entities"]["n_advertisers"])
        assert len(advertisers_df) == expected


class TestCampaignsSchema:
    REQUIRED_COLUMNS = {
        "campaign_id",
        "campaign_name",
        "advertiser_id",
        "retailer_id",
        "channel",
        "pricing_model",
        "bid_price_usd",
        "budget_usd",
        "daily_budget_usd",
        "target_audience_segment_id",
        "target_geo_id",
        "start_date",
        "end_date",
    }

    def test_schema(self, campaigns_df: pd.DataFrame) -> None:
        assert self.REQUIRED_COLUMNS.issubset(campaigns_df.columns)

    def test_key_counts_and_fk(self, campaigns_df: pd.DataFrame, advertisers_df: pd.DataFrame) -> None:
        entities = _simulation_bounds()["entities"]
        assert campaigns_df["campaign_id"].is_unique
        assert campaigns_df["campaign_name"].astype(str).is_unique
        assert len(campaigns_df) == int(entities["n_campaigns"])
        assert set(campaigns_df["advertiser_id"]).issubset(set(advertisers_df["advertiser_id"]))

    def test_campaign_ranges_and_windows(self, campaigns_df: pd.DataFrame) -> None:
        bounds = _simulation_bounds()
        n_retailers = int(bounds["entities"]["n_retailers"])
        n_segments = int(bounds["entities"]["n_audience_segments"])
        n_geo = _geo_count()
        cal_start = bounds["calendar_start"]
        cal_end = bounds["calendar_end"]

        assert campaigns_df["retailer_id"].between(1, n_retailers).all()
        assert campaigns_df["target_audience_segment_id"].between(1, n_segments).all()
        assert campaigns_df["target_geo_id"].between(1, n_geo).all()
        assert campaigns_df["budget_usd"].gt(0).all()
        assert campaigns_df["daily_budget_usd"].gt(0).all()

        start_date = pd.to_datetime(campaigns_df["start_date"])
        end_date = pd.to_datetime(campaigns_df["end_date"])
        assert (start_date <= end_date).all()
        assert start_date.between(cal_start, cal_end).all()
        assert end_date.between(cal_start, cal_end).all()
