# Booking ETL (PySpark + Iceberg)

Local PySpark job that reads booking JSONL, transforms fields, converts revenue to USD, and writes an **Iceberg** table (Parquet data files) under `data/warehouse/`.

## Docs

| File | Purpose |
|------|---------|
| [README.md](README.md) | Overview, structure, how to run, pipeline diagram |
| [AGENTS.md](AGENTS.md) | **Rules for AI agents** — what to maintain and how |

## Job vs pipeline?

| Name | When to use |
|------|-------------|
| **`src/jobs/`** | Spark entrypoints you `spark-submit` / `python -m` — **this project uses jobs** |
| **`pipelines/`** | Orchestration (Airflow/Dagster DAGs) that *call* jobs — not the Spark code itself |

## Project structure

```text
config/
  settings.py                 # paths, Spark memory, Iceberg catalog/table
src/
  jobs/
    booking_etl.py            # runnable job (read → transform → write)
  core/
    spark_session.py          # SparkSessionFactory + Iceberg catalog
  readers/
    jsonl_reader.py           # JsonlReader
    mapping_reader.py         # MappingReader (object JSON → DataFrame)
  services/
    exchange_rates.py         # ExchangeRateService
  transforms/
    bookings.py               # BookingTransformer
  writers/
    iceberg_writer.py         # IcebergWriter (Parquet via Iceberg)
  mappings/                   # lookup JSON objects
  main.py                     # thin alias → jobs.booking_etl
data/
  raw/bookings.jsonl          # input
  warehouse/                  # Iceberg warehouse (Parquet + metadata)
spark-submit.sh
requirements.txt
AGENTS.md
```

## How to run

```bash
cd /path/to/pyspark-iceberg-project
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PYTHONPATH=.

python -m src.jobs.booking_etl
# or
./spark-submit.sh
```

First run may download the Iceberg Spark package from Maven (needs network).

> **Note:** `pyspark==4.1.3` is pinned so it matches `iceberg-spark-runtime-4.1_2.13`. PySpark 4.2 is not compatible with current Iceberg runtimes yet.

## Iceberg table

| Setting | Value |
|---------|--------|
| Catalog | `local` (Hadoop catalog) |
| Warehouse path | `data/warehouse/bookings/` (no `default` namespace) |
| Table | `local.bookings` |
| Write | **MERGE INTO** on `transaction_id` |
| Data format | **Parquet** |
| Maintenance | `rewrite_data_files` + `expire_snapshots` (retain last 5) |

`data/` is gitignored (raw + warehouse stay local).

Read back:

```python
spark.table("local.bookings").show()
```

---

## Pipeline at a glance

```mermaid
flowchart TD
  A[Start job] --> B[Create SparkSession + Iceberg catalog]
  B --> C[JsonlReader reads bookings.jsonl]
  C --> D[BookingTransformer.transform]
  D --> E[Select / clean / map columns]
  E --> F[Add revenue_usd]
  F --> G[ExchangeRateService]
  G --> H{Currency in cache?}
  H -->|yes| I[Use cached rate]
  H -->|no| J[Call API once and cache in memory]
  I --> K[Join rates to bookings]
  J --> K
  K --> L[revenue_usd = revenue x rate]
  L --> M[IcebergWriter MERGE INTO + expire snapshots]
  M --> N[Parquet under data/warehouse/bookings]
  N --> O[show / count from local.bookings]
```

---

## DAG vs no DAG

Same job either way: `python -m src.jobs.booking_etl`.

| | Without DAG | With DAG |
|--|-------------|----------|
| How | Run the job yourself | Scheduler runs the same job module |
| Logic | Unchanged | Unchanged |

---

## Maintaining this project

Follow **[AGENTS.md](AGENTS.md)**.
