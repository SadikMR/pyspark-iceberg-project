#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  export JAVA_HOME
fi

# shellcheck disable=SC1091
source "$ROOT/.venv/bin/activate"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

exec python -m src.jobs.booking_etl
