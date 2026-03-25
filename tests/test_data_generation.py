"""
Validate synthetic dimension CSVs under ``data/synthetic/`` against project configs and generator contracts.

Run data generators first if files are missing::

    python scripts/generate_members.py
    python scripts/generate_advertisers.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC_DIR = REPO_ROOT / "data" / "synthetic"
MEMBERS_PATH = SYNTHETIC_DIR / "members.csv"
ADVERTISERS_PATH = SYNTHETIC_DIR / "advertisers.csv"
SIMULATION_CONFIG = REPO_ROOT / "configs" / "simulation_config.yaml"
EXPERIMENT_CONFIG = REPO_ROOT / "configs" / "experiment_config.yaml"

# Defaults if YAML cannot be read (match checked-in configs).
_DEFAULT_ENTITIES = {"n_retailers": 2, "n_advertisers": 8, "n_audience_segments": 5}
_DEFAULT_GEO = {"n_treatment_geos": 40, "n_control_geos": 8}
_DEFAULT_CAL_START = "2024-01-01"


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
    data = _load_yaml(SIMULATION_CONFIG)
    sim = data.get("simulation", data) if data else {}
    entities = {**_DEFAULT_ENTITIES, **sim.get("entities", {})}
    cal = sim.get("calendar", {})
    start = cal.get("start_date", _DEFAULT_CAL_START)
    return {"entities": entities, "calendar_start": pd.Timestamp(start)}


def _experiment_geo_count() -> int:
    data = _load_yaml(EXPERIMENT_CONFIG)
    exp = data.get("experiment", data) if data else {}
    geo = {**_DEFAULT_GEO, **exp.get("geo", {})}
    return int(geo["n_treatment_geos"]) + int(geo["n_control_geos"])


def _require_csv(path: Path) -> None:
    if not path.is_file():
        pytest.skip(f"Missing {path}; run the matching script under scripts/ to generate it.")


@pytest.fixture
def members_df() -> pd.DataFrame:
    _require_csv(MEMBERS_PATH)
    return pd.read_csv(MEMBERS_PATH)


@pytest.fixture
def advertisers_df() -> pd.DataFrame:
    _require_csv(ADVERTISERS_PATH)
    return pd.read_csv(ADVERTISERS_PATH)


class TestMembersSchema:
    """Column contract for ``members.csv`` (see ``scripts/generate_members.py``)."""

    REQUIRED_COLUMNS = (
        "member_id",
        "retailer_id",
        "audience_segment_id",
        "primary_geo_id",
        "signup_date",
        "outcome_currency",
    )

    def test_file_exists(self) -> None:
        _require_csv(MEMBERS_PATH)

    def test_non_empty(self, members_df: pd.DataFrame) -> None:
        assert len(members_df) >= 1

    def test_schema_columns(self, members_df: pd.DataFrame) -> None:
        missing = set(self.REQUIRED_COLUMNS) - set(members_df.columns)
        assert not missing, f"Missing columns: {sorted(missing)}"

    def test_member_id_unique_and_integer_like(self, members_df: pd.DataFrame) -> None:
        assert members_df["member_id"].is_unique
        assert pd.api.types.is_integer_dtype(members_df["member_id"])

    def test_foreign_key_ranges(self, members_df: pd.DataFrame) -> None:
        bounds = _simulation_bounds()
        ent = bounds["entities"]
        n_r = int(ent["n_retailers"])
        n_seg = int(ent["n_audience_segments"])
        n_geo = _experiment_geo_count()

        assert members_df["retailer_id"].between(1, n_r).all()
        assert members_df["audience_segment_id"].between(1, n_seg).all()
        assert members_df["primary_geo_id"].between(1, n_geo).all()

    def test_signup_before_simulation_window(self, members_df: pd.DataFrame) -> None:
        start = _simulation_bounds()["calendar_start"]
        signup = pd.to_datetime(members_df["signup_date"])
        assert signup.notna().all()
        assert (signup < start).all()

    def test_outcome_currency_non_empty(self, members_df: pd.DataFrame) -> None:
        cur = members_df["outcome_currency"].astype(str).str.strip()
        assert cur.ne("").all()


class TestAdvertisersSchema:
    """Column contract for ``advertisers.csv`` (see ``scripts/generate_advertisers.py``)."""

    REQUIRED_COLUMNS = (
        "advertiser_id",
        "advertiser_name",
        "retailer_id",
        "vertical_code",
        "created_at",
    )

    def test_file_exists(self) -> None:
        _require_csv(ADVERTISERS_PATH)

    def test_non_empty(self, advertisers_df: pd.DataFrame) -> None:
        assert len(advertisers_df) >= 1

    def test_schema_columns(self, advertisers_df: pd.DataFrame) -> None:
        missing = set(self.REQUIRED_COLUMNS) - set(advertisers_df.columns)
        assert not missing, f"Missing columns: {sorted(missing)}"

    def test_advertiser_id_unique_and_integer_like(self, advertisers_df: pd.DataFrame) -> None:
        assert advertisers_df["advertiser_id"].is_unique
        assert pd.api.types.is_integer_dtype(advertisers_df["advertiser_id"])

    def test_row_count_matches_config(self, advertisers_df: pd.DataFrame) -> None:
        expected = int(_simulation_bounds()["entities"]["n_advertisers"])
        assert len(advertisers_df) == expected

    def test_retailer_id_range(self, advertisers_df: pd.DataFrame) -> None:
        n_r = int(_simulation_bounds()["entities"]["n_retailers"])
        assert advertisers_df["retailer_id"].between(1, n_r).all()

    def test_advertiser_name_unique(self, advertisers_df: pd.DataFrame) -> None:
        assert advertisers_df["advertiser_name"].astype(str).is_unique

    def test_vertical_code_nonempty(self, advertisers_df: pd.DataFrame) -> None:
        v = advertisers_df["vertical_code"].astype(str).str.strip()
        assert v.ne("").all()

    def test_created_at_before_or_on_calendar_start(self, advertisers_df: pd.DataFrame) -> None:
        start = _simulation_bounds()["calendar_start"]
        created = pd.to_datetime(advertisers_df["created_at"])
        assert created.notna().all()
        assert (created.dt.normalize() <= start.normalize()).all()

    def test_round_robin_retailer_balance(self, advertisers_df: pd.DataFrame) -> None:
        """Generator assigns retailers round-robin; counts per retailer should match within one."""
        counts = advertisers_df.groupby("retailer_id").size()
        assert counts.max() - counts.min() <= 1
