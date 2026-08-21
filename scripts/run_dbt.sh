#!/usr/bin/env bash
# Load repository .env and run dbt against the local project/profile.
# Usage: scripts/run_dbt.sh debug|run|test|docs generate|...

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
  echo "POSTGRES_PASSWORD is not set. Copy .env.example to .env first." >&2
  exit 1
fi

cd "$ROOT/dbt"
exec dbt "$@" --project-dir "$ROOT/dbt" --profiles-dir "$ROOT/dbt"
