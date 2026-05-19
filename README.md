# IDX-Stream: Real-Time Sectoral Analytics Engine

## Overview

**IDX-Stream** adalah pipeline real-time hybrid untuk memonitor Top 5 saham dari setiap sektor bisnis di Bursa Efek Indonesia (IDX). Data diambil dari Yahoo Finance (`yfinance`), dialirkan melalui Kafka dan Spark di lokal, lalu disimpan ke Google BigQuery untuk analisis lanjutan menggunakan dbt.

**Stack:** Python · yfinance · Apache Kafka (KRaft) · Apache Spark · Google BigQuery · Docker

## Architecture

```
[Producer] ──JSON/9092──> [Kafka] ──Stream/29092──> [Spark] ──BigQuery Connector──> [GCP BigQuery]
     │                       │                           │
  yfinance              KRaft mode              Structured Streaming
  50+ ticker.JK         No Zookeeper            Exactly-once semantics
                                                    │
                                             Checkpointing
```

## Prerequisites

- Docker & Docker Compose (versi 3.8+)
- Google Cloud Platform account dengan BigQuery API enabled
- Service Account JSON key dengan akses BigQuery Data Editor

## Setup

### 1. Clone & Configure

```bash
git clone git@github.com:AdityaPratama575/idx-streaming-platform.git
cd idx-streaming-platform
cp .env.example .env
```

Edit `.env` — isi `GCP_PROJECT_ID`, dataset, table, dan konfigurasi lainnya.

### 2. GCP Service Account

Letakkan file `gcp-service-account.json` di root project (sudah di `.gitignore`).

### 3. Build & Run

```bash
docker-compose up --build
```

### 4. Verify

- **Spark Master UI:** http://localhost:8080
- **Spark Worker UI:** http://localhost:8081
- **Kafka topics:** `docker exec idx-kafka kafka-topics --bootstrap-server localhost:29092 --list`
- **Kafka message count:** `docker exec idx-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic idx_sector_ticks --time -1`
- **Producer logs:** `docker logs -f idx-producer`
- **Spark processor logs:** `docker logs -f idx-spark-processor`
- **BigQuery — cek data terbaru:**
  ```sql
  SELECT ticker, sector, timestamp, close, volume
  FROM `your-project.idx_stock_data.top_sector_ticks`
  WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  ORDER BY timestamp DESC
  LIMIT 20;
  ```

### 5. Shutdown

```bash
docker-compose down          # Stop semua container, data tetap aman
docker-compose down -v       # Stop + hapus volumes (checkpoint & Kafka data)
```

## Project Structure

```
idx-streaming-platform/
├── .env                          # Secrets (gitignored)
├── .env.example                  # Template konfigurasi
├── .gitignore
├── .dockerignore                  # Build context filter
├── gcp-service-account.json      # Credentials GCP (gitignored)
├── docker-compose.yml            # Orkestrasi 5 service
├── Dockerfile.producer           # Image untuk producer
├── Dockerfile.spark              # Image untuk spark processor
├── requirements-producer.txt     # Dependencies producer
├── requirements-spark.txt        # Dependencies spark
├── producer.py                   # Python: yfinance → Kafka
├── spark_processor.py            # Spark: Kafka → BigQuery
├── top5_saham_ihsg_by_sector_market_cap.py  # Daftar ticker per sektor
├── README.md
├── docs/
│   ├── agents.md                 # Aturan operasional agent
│   └── instruction.md            # Dokumentasi arsitektur
└── issue/
    ├── 01-docker-compose.md
    ├── 02-dockerfiles-dan-dependencies.md
    ├── 03-producer.md
    ├── 04-spark-processor.md
    ├── 05-readme.md
    ├── 06-spark-processor-bugfixes.md
    ├── 07-producer-edge-cases.md
    ├── 08-infrastructure-gaps.md
    └── 09-documentation-operational.md
```

## Configuration

| Variable | Deskripsi | Contoh |
|---|---|---|
| `GCP_PROJECT_ID` | Project ID Google Cloud | `idx-analytics-platform` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path ke service account JSON | `./gcp-service-account.json` |
| `GCP_BIGQUERY_DATASET` | Nama dataset BigQuery | `idx_stock_data` |
| `GCP_BIGQUERY_TABLE` | Nama table BigQuery | `top_sector_ticks` |
| `KAFKA_BOOTSTRAP_SERVERS` | Alamat broker Kafka (internal Docker) | `kafka:29092` |
| `KAFKA_TOPIC` | Kafka topic name | `idx_sector_ticks` |
| `SPARK_APP_NAME` | Nama aplikasi Spark | `RealTimeIDXProcessor` |
| `SPARK_CHECKPOINT_DIR` | Direktori checkpoint Spark | `/tmp/spark-checkpoints/idx_stock` |
| `FETCH_INTERVAL_SECONDS` | Interval fetch data (detik) | `60` |
| `YFINANCE_DELAY_SECONDS` | Delay antar request Yahoo Finance (detik) | `0.5` |

## Troubleshooting

- **Kafka tidak terkoneksi:** Pastikan semua service di network `idx-network`. Cek log: `docker logs idx-kafka`.
- **BigQuery permission error:** Verifikasi `gcp-service-account.json` punya role **BigQuery Data Editor**.
- **Spark job stuck:** Hapus folder checkpoint: `docker volume rm spark_checkpoints`, lalu `docker-compose up --build`.
- **yfinance gagal fetch:** Beberapa ticker mungkin tidak aktif. Cek log: `docker logs idx-producer`.
- **DLQ (Dead Letter Queue):** Jika ada malformed JSON, cek topic `idx_sector_ticks_dlq`: `docker exec idx-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic idx_sector_ticks_dlq --from-beginning`.
