# AGENTS.md — codebase maintenance rules

Instructions for **AI agents** (and humans) working on this PySpark + Iceberg booking ETL.

Pipeline overview and run steps live in [README.md](README.md). This file is the **source of truth for what to maintain and how**.

---

## What this project is

A small **class-based PySpark ETL**:

1. Read booking JSONL  
2. Transform / map columns  
3. Add `revenue_usd` via FX API (in-memory cache)  
4. Write **Iceberg** table `local.bookings` (Parquet under `data/warehouse/bookings/`) via **MERGE INTO**

Stay **simple and industry-standard**. Do not over-engineer.

---

## Layout — keep this structure

```text
config/settings.py           # constants (paths, memory, Iceberg catalog/table)
src/main.py                  # entrypoint — CLI + dispatch by --cron-name
src/jobs/booking_etl.py      # booking job logic
src/core/spark_session.py    # SparkSessionFactory + Iceberg catalog
src/readers/                 # JsonlReader, MappingReader
src/services/                # ExchangeRateService (FX)
src/transforms/              # BookingTransformer
src/writers/                 # IcebergWriter
src/utils/                   # shared helpers (e.g. @timed runtime)
src/mappings/                # plain object JSON lookups
```

| Put here | Kind of code |
|----------|----------------|
| `src/main.py` | CLI args + start / dispatch by `--cron-name` |
| `src/jobs/` | Job logic called from main |
| `src/core/` | Spark session / shared runtime |
| `src/readers/` | Read sources into DataFrames |
| `src/services/` | HTTP / FX / external integrations |
| `src/transforms/` | DataFrame business transforms |
| `src/writers/` | Iceberg / other sinks |
| `config/settings.py` | Paths and simple constants only |

Use **`main.py`** as the only user entrypoint. Keep job logic under **`jobs/`**. Reserve **`pipelines/`** for orchestrators (Airflow, etc.) if added later.

Mapping JSON files are plain objects (`{"m": "mobile"}`). `MappingReader` turns them into a DataFrame for joins.

Do **not** add unused config modules, DI frameworks, or factories-of-factories.

---

## Rules to maintain

### Architecture

- Stay **class-based** end to end:
  - `Application` (CLI + dispatch)
  - `BookingEtlJob`
  - `SparkSessionFactory`, `JsonlReader`, `MappingReader`, `ExchangeRateService`, `BookingTransformer`, `IcebergWriter`
- One clear responsibility per class.
- `Application` dispatches by `--cron-name`; job classes live under `src/jobs/`.

### Spark / data

- Prefer `import pyspark.sql.functions as F` and `import pyspark.sql.types as T`.
- Dimension lookups = **DataFrames + `F.broadcast` join**.
- Do **not** use `create_map` from Python dicts for lookups.
- Do **not** call the FX API per row.

### Exchange rates

- Currencies from **distinct `currency` in the DataFrame** — never hardcode lists.
- **In-memory** cache on `ExchangeRateService` only.
- Keep **both** `revenue` and `revenue_usd`.

### Iceberg

- Table: `local.bookings` → files under `data/warehouse/bookings/` (no `default` namespace).
- Writes use **MERGE INTO** on `transaction_id` (create table once if missing).
- Default data format: **Parquet**; format-version 2.
- After write: `rewrite_data_files` then `expire_snapshots(retain_last=5)`.
- Writers stay small and simple in `src/writers/`.
- Entire `data/` directory is gitignored.

### Transforms

- Separate methods by concern (booking fields, user fields, label extract, maps, casts, revenue USD).

### Config / Spark session

- Session configs on the builder + `settings.py`.
- Driver memory + Iceberg `--packages` via `PYSPARK_SUBMIT_ARGS` before JVM start (in the job module).

---

## Do / don't

| Do | Don't |
|----|--------|
| Jobs under `src/jobs/` | Spark entrypoint under a vague `pipelines/` folder |
| Writers under `src/writers/` | Write Iceberg ad‑hoc only in the job file |
| Broadcast joins for maps | Per-row HTTP calls |
| Plain object mapping JSON | Force `key`/`value` fields in JSON files |
| Update README diagram if flow changes | Add Airflow code unless asked |

---

## When you change the pipeline

1. Prefer extending an existing class, or add one small class in the right folder.  
2. Wire it in `src/jobs/booking_etl.py`.  
3. Update the mermaid diagram in [README.md](README.md) if the flow changes.  
4. Keep this `AGENTS.md` up to date if maintenance rules change.

---

## How to run (reminder)

```bash
source .venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PYTHONPATH=.

python -m src.main \
  --cron-name booking \
  --updated_from 2026-07-12 \
  --updated_to 2026-07-13

python -m src.main \
  --cron-name migrate_postgres
```
