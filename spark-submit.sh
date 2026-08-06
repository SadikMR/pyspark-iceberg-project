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

if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT/.venv/bin/activate"
else
  echo "Virtual environment not found at $ROOT/.venv" >&2
  exit 1
fi

export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export PYSPARK_PYTHON="$ROOT/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$ROOT/.venv/bin/python"

SPARK_HOME="$($ROOT/.venv/bin/python - <<'PY'
import os
import pyspark
print(os.path.dirname(os.path.realpath(pyspark.__file__)))
PY
)"
export SPARK_HOME

exec "$ROOT/.venv/bin/spark-submit" \
  --master "local[*]" \
  --driver-memory 4g \
  --executor-memory 4g \
  --packages "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0" \
  "${ROOT}/src/main.py" \
  "$@"
