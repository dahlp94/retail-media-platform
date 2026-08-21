#!/usr/bin/env bash
# Rebuild governed warehouse models, run dbt tests, then attach Python
# inference and Stage 4 decisions. Does not regenerate synthetic source data.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

"$ROOT/scripts/run_dbt.sh" run
"$ROOT/scripts/run_dbt.sh" test

python scripts/run_incrementality.py --export-csv
python scripts/run_experiment_decisions.py --export-csv
python -m pytest -q
