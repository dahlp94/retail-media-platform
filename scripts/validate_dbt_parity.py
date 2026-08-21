"""
Compare dbt-built incrementality marts to the frozen pandas ITT reference.

Does not regenerate synthetic data. Requires PostgreSQL and `dbt run`.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(pytest.main(["-q", str(ROOT / "tests" / "test_dbt_parity.py")]))


if __name__ == "__main__":
    main()
