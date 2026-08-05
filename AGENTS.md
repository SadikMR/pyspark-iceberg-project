# AGENTS.md — codebase maintenance rules

Instructions for **AI agents** (and humans) working on this PySpark booking ETL.

Pipeline overview and run steps live in [README.md](README.md). This file is the **source of truth for what to maintain and how**.

---

## What this project is

A small **class-based PySpark ETL**:

1. Read booking JSONL  
2. Transform / map columns  
3. Add `revenue_usd` via FX API (in-memory cache)  
4. Print schema / sample / count  

Stay **simple and industry-standard**. Do not over-engineer.

---

## Layout — keep this structure

```text
config/settings.py           # constants only (paths, memory, app name)
src/main.py                  # wire classes + run (no heavy logic)
src/core/spark_session.py    # SparkSessionFactory
src/readers/                 # input only (JsonlReader)
src/services/                # external APIs / services (ExchangeRateService)
src/transforms/              # DataFrame transforms (BookingTransformer)
src/mappings/                # small JSON lookup tables
```

| Put here | Kind of code |
|----------|----------------|
| `src/core/` | Spark session / shared runtime |
| `src/readers/` | Reading files or sources into DataFrames |
| `src/services/` | HTTP, FX, other external integrations |
| `src/transforms/` | Pure DataFrame business transforms |
| `config/settings.py` | Paths and simple constants only |
| `src/main.py` | Orchestration only |

Mapping JSON files are plain objects (`{"m": "mobile"}`), not `key`/`value` rows. `MappingReader` turns them into a DataFrame for joins.

Do **not** add unused config modules, DI frameworks, or factories-of-factories.

---

## Rules to maintain

### Architecture

- Stay **class-based**: `SparkSessionFactory`, `JsonlReader`, `ExchangeRateService`, `BookingTransformer`.
- One clear responsibility per class.
- Wire new pieces in `main.py` only.

### Spark / data

- Prefer `import pyspark.sql.functions as F` and `import pyspark.sql.types as T` (aliases, not dozens of individual imports).
- Dimension lookups = **DataFrames + `F.broadcast` join** (status, device, region, FX rates).
- Do **not** use `create_map` from Python dicts for lookups.
- Do **not** call the FX API per row — once per distinct currency, then join.

### Exchange rates

- Currencies come from **distinct `currency` values in the DataFrame** — never hardcode `["USD", "EUR", ...]`.
- Cache rates in an **in-memory dict** on `ExchangeRateService` for the process lifetime.
- No FX cache files unless the user explicitly asks.
- Keep **both** `revenue` and `revenue_usd`.

### Transforms

- Separate methods by concern (booking fields, user fields, label extract, status/device/region maps, casts, revenue USD).
- Meaningful names only.

### Config / Spark session

- Session configs on the builder + `settings.py`.
- Driver memory needs `PYSPARK_SUBMIT_ARGS` before JVM start (already in `main.py`) — do not remove that without a replacement.

---

## Do / don't

| Do | Don't |
|----|--------|
| Simple classes in the folders above | Abstract base-class hierarchies |
| Broadcast joins for small maps | Per-row HTTP calls |
| Constants in `settings.py` | Dead `spark_config` modules |
| FX under `src/services/` | Bury FX only inside `main` |
| Update README diagram if flow changes | Add Airflow/DAG code unless asked |

---

## When you change the pipeline

1. Prefer extending an existing class, or add one small class in the right folder.  
2. Wire it in `src/main.py`.  
3. Update the mermaid diagram in [README.md](README.md) if the flow changes.  
4. Keep this `AGENTS.md` up to date if maintenance rules change.

---

## How to run (reminder)

```bash
source .venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PYTHONPATH=.
python -m src.main
```
