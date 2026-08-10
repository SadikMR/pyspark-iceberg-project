# AGENTS.md — codebase maintenance rules

Instructions for **AI agents** (and humans) working on this project.

Pipeline overview: [README.md](README.md). Keep code **simple** — no over-engineering.

---

## What this project is

Two crons:

1. **`booking`** — JSONL → transform → Iceberg  
2. **`migrate_postgres`** — Iceberg → Postgres (snapshot tracking + upsert)

---

## Layout

```text
config/settings.py
src/main.py
src/jobs/booking_etl.py
src/jobs/migrate_postgres.py
src/core/spark_session.py
src/readers/          # JsonlReader, MappingReader, IcebergReader
src/services/         # ExchangeRateService
src/transforms/       # BookingTransformer
src/writers/          # IcebergWriter, PostgresWriter
src/mappings/
docker-compose.yml
```

| Folder | Use |
|--------|-----|
| `jobs/` | Orchestration only |
| `readers/` | Read sources |
| `writers/` | Write sinks (Iceberg, Postgres) |
| `services/` | FX API |
| `transforms/` | Business transforms |

No ORM. No extra `db/` or tracking service layers.

---

## Rules

- Class-based, one clear job per `--cron-name`.
- Postgres: one `PostgresWriter` (schema + tracking + upsert).
- Migration: no tracking → full upsert; same snapshot → skip; else upsert then save watermark.
- Watermark advances only after successful commit.
- Iceberg MERGE INTO on `transaction_id`; `expire_snapshots(retain_last=5)`.

---

## Run

```bash
docker compose up -d
./spark-submit.sh --cron-name booking --updated_from 2026-07-12 --updated_to 2026-07-13
./spark-submit.sh --cron-name migrate_postgres
```
