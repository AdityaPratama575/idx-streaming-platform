# IDX-Stream: Real-Time Sectoral Analytics Engine

**IDX-Stream** adalah pipeline real-time hybrid untuk memonitor Top 5 saham dari setiap sektor bisnis di Bursa Efek Indonesia (IDX). Data diambil dari Yahoo Finance (`yfinance`), dialirkan melalui Kafka dan Spark di lokal, lalu disimpan ke Google BigQuery untuk analisis lanjutan menggunakan dbt.

**Stack:** Python · yfinance · Apache Kafka (KRaft) · Apache Spark 3.4 · Google BigQuery · dbt · Docker · Terraform

## Architecture

```
                                 Producer Dedup + Off-Hours Scheduling
[Yahoo Finance] ──(51 ticker)──▶ [idx-producer] ───JSON──▶ [Kafka Broker]
                                                              │ 29092
                                                     ┌────────┴────────┐
                                                     ▼                 ▼
                                              [Spark Processor]    [DLQ Topic]
                                                     │
                                          Watermark + Dedup + DQ
                                                     │
                                          Parquet → GCS Temp Staging
                                                     │
                                               BigQuery LOAD Job
                                                     ▼
                                          [BigQuery: top_sector_ticks]
                                                     │
                                               dbt Transformation
                                                     ▼
                              [marts: top5, sector_perf, market_breadth]
```

## Prerequisites

- Docker & Docker Compose (versi 3.8+)
- Google Cloud Platform account dengan BigQuery API enabled
- Service Account JSON key dengan akses **BigQuery Data Editor** + **Storage Object Admin**
- GCS Bucket untuk temporary staging BigQuery
- (Optional) Python 3.9+ untuk lokal testing

## Quick Start

```bash
git clone git@github.com:AdityaPratama575/idx-streaming-platform.git
cd idx-streaming-platform
cp .env.example .env
# Edit .env — isi GCP_PROJECT_ID, dataset, table, bucket
# Letakkan gcp-service-account.json di root project
docker compose up --build
```

### Verify

| URL/Command | Deskripsi |
|---|---|
| http://localhost:8080 | Spark Master UI |
| http://localhost:8081 | Spark Worker UI |
| http://localhost:8082 | Airflow Webserver (admin/admin) |
| http://localhost:3000 | Grafana Dashboard (admin/admin) |
| http://localhost:9090 | Prometheus UI |
| `docker logs -f idx-producer` | Log producer real-time |
| `docker logs -f idx-spark-processor` | Log Spark streaming |
| `docker exec idx-kafka kafka-topics --bootstrap-server localhost:29092 --list` | Daftar Kafka topics |

## Fitur Utama

### Producer Optimization
- **Dedup cache**: Hanya kirim candle baru ke Kafka (hemat 99% traffic setelah batch pertama)
- **Off-hours scheduling**: Market hours (Mon-Fri 09:00-15:00 WIB) → fetch 60 detik, off-hours → fetch 1 jam
- **Timezone WIB**: Semua timestamp dalam `+07:00` (Asia/Jakarta)
- **Schema headers**: `schema_name` + `schema_version` di tiap Kafka record

### Spark Processing
- Structured Streaming dengan exactly-once semantics
- JSON parsing with PERMISSIVE mode (forward compatibility)
- NaN → NULL, null ticker filter, ISO 8601 timestamp parsing
- Watermark + `dropDuplicates` (30 menit window)
- Dead Letter Queue untuk malformed messages

## Project Structure

```
idx-streaming-platform/
├── .env                          # Secrets (gitignored)
├── docker-compose.yml            # Orkestrasi 5 service
├── producer.py                   # Python: yfinance → Kafka (dedup + off-hours)
├── spark_processor.py            # Spark: Kafka → BigQuery (PERMISSIVE mode)
├── top5_saham_ihsg_by_sector_market_cap.py  # 51 ticker × 11 sektor
│
├── tests/                        # Issue #10 — Unit & Integration Tests
│   ├── test_producer.py          # 14 test cases
│   ├── test_spark_processor.py   # 8 test cases (skip gracefully tanpa Java)
│   └── fixtures/                 # Sample payloads
│
├── .github/workflows/            # Issue #11 — CI/CD Pipeline
│   ├── ci.yml                    # Lint (ruff) + test producer + test spark
│   └── cd.yml                    # Build & push Docker ke GHCR
│
├── dbt/                          # Issue #12 — dbt Transformation Layer
│   ├── models/staging/           # Source definitions, stg_idx_sector_ticks
│   ├── models/intermediate/      # Daily stats, sector summary, rankings
│   ├── models/marts/             # Top 5, sector perf, market breadth, anomalies
│   └── seeds/                    # Sector mapping
│
├── schemas/                      # Issue #17 — Schema Registry
│   ├── stock_tick_v1.avsc        # Avro schema contract
│   └── CHANGELOG.md              # Version history
│
├── sql/                          # Issue #14 — Data Quality
│   ├── dq_check_results_ddl.sql  # DQ table DDL
│   └── dq_checks.sql             # OHLCV, stale data, null checks
│
├── airflow/dags/                 # Issue #15 — Airflow Orchestration
│   ├── idx_pipeline_daily.py     # Daily batch pipeline DAG
│   └── idx_streaming_monitor.py  # Health check DAG (10 menit)
│
├── monitoring/                   # Issue #13 — Observability
│   ├── prometheus.yml            # Scrape config
│   └── grafana/                  # Dashboard & datasource provisioning
│
├── terraform/                    # Issue #16 — IaC
│   ├── main.tf, bigquery.tf      # BigQuery dataset + table (partitioned + clustered)
│   ├── storage.tf                # GCS buckets with lifecycle
│   └── iam.tf                    # Service account + roles
│
└── utils/                        # Issue #20 — Secrets Management
    └── secrets.py                # Secret Manager + .env fallback
```

## Configuration

| Variable | Deskripsi | Contoh |
|---|---|---|
| `GCP_PROJECT_ID` | Project ID Google Cloud | `idx-analytics-platform` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path ke service account JSON | `./gcp-service-account.json` |
| `GCP_BIGQUERY_DATASET` | Nama dataset BigQuery | `idx_stock_data` |
| `GCP_BIGQUERY_TABLE` | Nama table BigQuery | `top_sector_ticks` |
| `GCS_TEMP_BUCKET` | Bucket GCS untuk staging | `idx-temp-staging` |
| `KAFKA_BOOTSTRAP_SERVERS` | Alamat broker Kafka | `kafka:29092` |
| `KAFKA_TOPIC` | Kafka topic name | `idx_sector_ticks` |
| `SPARK_APP_NAME` | Nama aplikasi Spark | `RealTimeIDXProcessor` |
| `SPARK_CHECKPOINT_DIR` | Direktori checkpoint | `/tmp/spark-checkpoints/idx_stock` |
| `FETCH_INTERVAL_SECONDS` | Interval fetch (market hours) | `60` |
| `YFINANCE_DELAY_SECONDS` | Delay antar request Yahoo | `0.5` |

## BigQuery Schema — `top_sector_ticks`

| Field | Type | Mode |
|---|---|---|
| `ticker` | STRING | NULLABLE |
| `sector` | STRING | NULLABLE |
| `timestamp` | TIMESTAMP | NULLABLE |
| `fetch_ts` | TIMESTAMP | NULLABLE |
| `open` | FLOAT | NULLABLE |
| `high` | FLOAT | NULLABLE |
| `low` | FLOAT | NULLABLE |
| `close` | FLOAT | NULLABLE |
| `volume` | INTEGER | NULLABLE |

Partitioned by `timestamp` (DAY), clustered by `ticker`, `sector`.

## dbt Models

| Layer | Model | Deskripsi |
|---|---|---|
| Staging | `stg_idx_sector_ticks` | Clean, cast, filter 90 hari |
| Intermediate | `int_daily_stock_stats` | OHLCV harian per ticker |
| Intermediate | `int_sector_daily_summary` | Agregasi per sektor |
| Intermediate | `int_ticker_rankings` | Ranking volume per sektor |
| Mart | `mrt_top5_sector_daily` | Top 5 saham per sektor |
| Mart | `mrt_sector_performance` | Sector heatmap hari ini |
| Mart | `mrt_market_breadth` | Advance/decline ratio |
| Mart | `mrt_volume_anomalies` | Volume spike detection |

## Troubleshooting

- **Kafka tidak terkoneksi:** `docker logs idx-kafka`. Pastikan semua container di `idx-network`.
- **BigQuery permission error:** Verifikasi service account punya role **BigQuery Data Editor** + **Storage Object Admin**.
- **Spark job stuck / data tidak masuk ke BQ:** Hapus checkpoint: `docker volume rm spark_checkpoints` lalu `docker compose up --build -d spark-processor`.
- **Producer skip semua candle:** Normal — dedup aktif. Tunggu candle baru dari yfinance (biasanya delay 5-30 menit).
- **Running tests:** `pip install -r requirements-test.txt && python3 -m pytest tests/ -v`.
- **dbt:** `cd dbt && dbt deps && dbt run --profiles-dir .`.

## Shutdown

```bash
docker compose down              # Stop, data tetap aman
docker compose down -v           # Stop + hapus volumes (checkpoint & Kafka data)
```
