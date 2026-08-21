"""
Publication check for processed measurement artifacts.

Structural only. Does not recompute incrementality or decisions.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.orchestration.checks import PipelineCheckError, validate_processed_outputs


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        summary = validate_processed_outputs()
    except PipelineCheckError as exc:
        logging.error("Processed output validation failed: %s", exc)
        raise SystemExit(1) from exc
    logging.info(
        "Publication check passed for %s campaigns.",
        summary["n_campaigns"],
    )


if __name__ == "__main__":
    main()
