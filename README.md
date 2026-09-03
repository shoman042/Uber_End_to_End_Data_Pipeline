# Uber NYC Rides Pipeline

A medallion-architecture (Bronze → Silver → Gold) data engineering pipeline built on the **Uber NYC 2014 pickups dataset**, combining a batch ingestion path and a simulated real-time streaming path into a unified data warehouse.

## Architecture

Raw trip data enters through two parallel paths that converge into a single Silver layer, then a dimensional Gold layer, then a Serving layer for querying and consumption.

```
Uber pickups dataset (CSV, NYC 2014)
        │
   ┌────┴─────┐
Batch path   Stream path
   │             │
Sqoop / NiFi   Kafka producer
   │             │
HDFS bronze    Spark streaming
   │             │
PySpark ETL    Micro-batch (geo-hash)
   └────┬─────┘
     Silver layer (unified rides table)
        │
     Gold layer (Fact_Trip + dimensions)
        │
     Serving layer (HiveQL / Presto)
        │
     Consumption (dashboards, ML, reports)
```

- **Batch path** — historical monthly files ingested via an Apache NiFi flow into HDFS Bronze, then cleaned and geo-hashed with PySpark.
- **Stream path** — the final month (December 2014) is replayed through a Kafka producer, consumed, and processed with Spark Structured Streaming into HDFS Bronze.
- All layers (Bronze, Silver, Gold, checkpoints) live on **HDFS**; local disk is used only for code, config, and docs.

## Data Source

Uber NYC pickups dataset — 11 monthly raw CSV files (`uber_raw_data_02_14.csv` … `uber_raw_data_12_14.csv`) plus a `manifest.csv`.

Fields: `trip_id`, `start_lat`, `start_lon`, `end_lat`, `end_lon`, `start_time`, `end_time`, `distance`.

## Streaming Component

- **Producer** — reads `uber_raw_data_12_14.csv` unmodified and publishes to Kafka, pacing sends itself rather than following the original timestamps. Loops the file indefinitely until stopped.
- **Consumer** — writes streamed records into `bronze/streaming` on HDFS so Spark can pick them up as soon as a new file lands.
- **Spark Structured Streaming** — processes new files into Silver.
- **Deduplication** — because the producer loops indefinitely (~8h per cycle), a time-bounded watermark isn't enough. Instead, a persistent *seen-trip-ids* file on HDFS is checked/updated per micro-batch via `foreachBatch`, guaranteeing zero duplicates in Silver regardless of elapsed time.

## Batch Ingestion Flow (NiFi)

| Processor | Role |
|---|---|
| `ListFile` | Detects source files to ingest |
| `DetectDuplicate` | Skips files already ingested |
| `FetchFile` | Retrieves file content |
| `PutHDFS` | Writes content to HDFS |
| `RetryFlowFile` | Retries failed attempts |
| `PutFile_DLQ` | Routes repeated failures to a dead-letter queue |

## Batch Component

Bronze batch data is processed by a Spark batch ETL job designed to match the **stream path's output shape**, so batch-origin and stream-origin records can later be merged into one unified Silver layer (kept as separate files within Silver).

## Data Warehouse (Gold Layer)

Star schema, built incrementally via `gold_etl.py` using the same seen-id tracking pattern as the streaming job (one tracking file per dimension).

**`FACT_TRIP`**
`trip_id (PK)`, `date_key (FK)`, `time_key (FK)`, `start_geo_key (FK)`, `end_geo_key (FK)`, `duration_bucket_key (FK)`, `start_time`, `trip_duration_sec`, `trip_count`, `is_same_cell`

**Dimensions**
- `DIM_DATE` — date_key (PK), full_date, day, day_name, day_of_week, is_weekend, week_of_year, month, month_name, quarter, year
- `DIM_TIME` — time_key (PK), hour, minute, period, is_rush_hour
- `DIM_GEOGRAPHY` — geo_hash (PK), lat, lon, parent_geo_res6, parent_geo_res5
- `DIM_DURATION_BUCKET` — duration_bucket_key (PK), bucket_label, min_seconds, max_seconds

## Serving Layer

Hive external tables over Silver and Gold, queried via **HiveQL** and **Presto**. Final Gold tables are exported as coalesced CSVs for teammates building dashboards.

## Pipeline Control Panel

A single control panel triggers each stage independently or end-to-end:

- **Run Full Pipeline** — starts batch + streaming together, then Silver Union → Gold ETL → Export, in order; stops on first failure
- **Streaming** — start / stop
- **Batch** — start / stop
- **Silver Union** — merges batch-origin and stream-origin Silver data
- **Gold Layer** — runs `gold_etl.py`
- **Export** — exports final Gold CSVs
- **Hive Query** — ad-hoc HiveQL box (supports multiple `;`-separated statements)

## Dashboards

Three-page analytical dashboard built on the Gold layer:

1. **Overview** — total trips, average duration, trips by day/month, weekday vs. weekend split, trips by hour
2. **Time & Geography** — geographic trip distribution, top start/end regions, route breakdown, rush-hour split
3. **Duration Analysis** — trip distribution and average duration by duration bucket

## Project Structure

```
project-root/
├── bronze/
│   ├── streaming/      # Kafka consumer output
│   └── batch/          # NiFi batch ingestion output
├── silver/              # Unified rides table (batch + stream, separate files)
├── gold/                 # Dimensional model (Fact_Trip + dimensions)
├── checkpoints/          # Spark streaming checkpoints
├── nifi/                 # NiFi flow definitions
├── spark/                # Streaming + batch ETL + gold_etl.py
├── kafka/                # Producer / consumer
└── control_panel/        # Pipeline orchestration UI
```

> All of `bronze/`, `silver/`, `gold/`, and `checkpoints/` are stored on **HDFS**, not local disk — the tree above reflects logical layout, not the local filesystem.

## Status

- ✅ Streaming path (Kafka → Spark Structured Streaming → Silver) — complete, including deduplication
- ✅ Batch data uploaded to Bronze on HDFS
- 🔄 Batch ETL (aligning output shape with the stream path) — in progress
- ⏳ Silver union, Gold layer, and export — pending batch completion

## Key Design Decisions

- **Why not a watermark for dedup?** The producer's loop cycle (~8h) outlasts any practical watermark window, so a persistent seen-id file on HDFS is used instead — durable across the whole run, not just a time window.
- **Why everything on HDFS?** Keeps the pipeline consistent with the original architecture (Bronze on HDFS) and keeps local disk limited to code/config/docs.
- **Why match batch output to stream output?** So the two independently-built paths can be unioned into Silver without a reconciliation step.
