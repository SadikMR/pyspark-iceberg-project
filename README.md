# Booking ETL (PySpark + Iceberg → Postgres)

Two cron jobs:

1. **`booking`** — read JSONL by date → transform → MERGE INTO Iceberg  
2. **`migrate_postgres`** — sync Iceberg → PostgreSQL using a snapshot tracking table  

## Prerequisites

- Python 3.12+ with project venv (`.venv`)
- Java 17
- Docker (for local Postgres)

```bash
cd /path/to/pyspark-iceberg-project
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Project structure

```text
config/settings.py
src/main.py                      # CLI entry (--cron-name)
src/jobs/
  booking_etl.py                 # booking cron
  migrate_postgres.py            # migrate_postgres cron
src/core/spark_session.py
src/readers/                     # Jsonl, Mapping, Iceberg
src/services/exchange_rates.py
src/transforms/bookings.py
src/writers/
  iceberg_writer.py
  postgres_writer.py             # schema + tracking + upsert
src/mappings/
data/raw/bookings.jsonl          # input (gitignored)
data/warehouse/                  # Iceberg warehouse (gitignored)
docker-compose.yml               # Postgres on localhost:5434
spark-submit.sh
```

## Setup (once)

```bash
source .venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PYTHONPATH=.

# Start Postgres (host port 5434 → container 5432)
docker compose up -d
docker compose ps
```

Postgres connection (from `config/settings.py`):

```text
postgresql://postgres:postgres@localhost:5434/iceberg_db
```

## Commands

Preferred (sets Iceberg JAR + memory via `spark-submit`):

### 1) Booking — transform one date window into Iceberg

```bash
./spark-submit.sh \
  --cron-name booking \
  --updated_from 2026-05-07 \
  --updated_to 2026-05-07
```

Dates are inclusive (`YYYY-MM-DD`). Use a 1-day window for a quick test.

### 2) Migrate — Iceberg → Postgres

```bash
./spark-submit.sh --cron-name migrate_postgres
```

No date args. Compares Iceberg latest snapshot to `migration_tracking`; upserts if needed.

### Optional: run via Python module

Same jobs (JAR/memory come from `PYSPARK_SUBMIT_ARGS` in `src/main.py`):

```bash
source .venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PYTHONPATH=.

python -m src.main \
  --cron-name booking \
  --updated_from 2026-05-07 \
  --updated_to 2026-05-07

python -m src.main --cron-name migrate_postgres
```

## CLI arguments

| Argument | Required | Used by | Example |
|----------|----------|---------|---------|
| `--cron-name` | yes | both | `booking` or `migrate_postgres` |
| `--updated_from` | yes for `booking` | booking | `2026-05-07` |
| `--updated_to` | yes for `booking` | booking | `2026-05-07` |

## Typical flow

```bash
# Day 1 window → Iceberg
./spark-submit.sh --cron-name booking --updated_from 2026-05-07 --updated_to 2026-05-07

# Sync to Postgres
./spark-submit.sh --cron-name migrate_postgres

# Later: another day → Iceberg
./spark-submit.sh --cron-name booking --updated_from 2026-05-08 --updated_to 2026-05-08

# Sync again (upserts current Iceberg state; updates tracking)
./spark-submit.sh --cron-name migrate_postgres

# Run migrate again with no new booking → skip
./spark-submit.sh --cron-name migrate_postgres
```

## migrate_postgres behavior

Table `migration_tracking` stores the last Iceberg `snapshot_id` applied to Postgres.

| Tracking | Iceberg latest | Action |
|----------|----------------|--------|
| no row | any | full upsert of current table |
| same as latest | — | skip |
| different | newer | upsert current table, then save latest |

- Upsert key: `transaction_id` (`ON CONFLICT DO UPDATE`)
- Watermark updates only after a successful upsert + commit

## Iceberg table

| Setting | Value |
|---------|--------|
| Catalog / table | `local.bookings` |
| Warehouse | `data/warehouse/bookings/` |
| Write | MERGE INTO on `transaction_id` |
| Format | Parquet, format-version 2 |

## Pipeline

```mermaid
flowchart TD
  A[booking cron] --> B[JSONL by date]
  B --> C[transform + FX]
  C --> D[Iceberg MERGE INTO]
  D --> E[migrate_postgres cron]
  E --> F{tracking vs Iceberg snapshot}
  F -->|none or changed| G[upsert Postgres bookings]
  G --> H[update migration_tracking]
  F -->|same| I[skip]
```

## Notes

- First Spark run may download the Iceberg package from Maven (needs network).
- `pyspark==4.1.3` matches `iceberg-spark-runtime-4.1_2.13`.
- `data/` is gitignored (raw input + warehouse stay local).

## Maintaining

Follow **[AGENTS.md](AGENTS.md)**.
