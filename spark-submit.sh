#!/usr/bin/env bash
# Runtime-only flags (JAR + memory). All other Spark/Iceberg config
# lives in config/settings.py + src/core/spark_session.py.
#
# Usage:
#   ./spark-submit.sh \
#     --cron-name booking \
#     --updated_from 2026-07-12 \
#     --updated_to 2026-07-13

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

# Only what must be set before/at JVM start:
#   --packages  → Iceberg runtime JAR
#   --*-memory  → heap size
exec spark-submit \
  --master "local[*]" \
  --driver-memory 4g \
  --executor-memory 4g \
  --packages "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0" \
  "${ROOT}/src/main.py" \
  "$@"
