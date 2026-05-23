# Pipeline Documentation — IDX-Stream Analytics Engine

## 1. Arsitektur Pipeline

```
                              ┌─────────────────────────────────────────────────────┐
                              │                     Docker Compose                   │
                              │                                                     │
┌──────────────┐     ┌───────▼──────┐     ┌──────────────────┐     ┌──────────────┐│
│  Yahoo       │     │   Producer   │     │    Kafka         │     │   Spark      ││
│  Finance     │────▶│  (Python)    │────▶│  (KRaft Mode)    │────▶│  Processor   ││
│  (yfinance)  │     │              │     │                  │     │              ││
└──────────────┘     │ dedup + off- │     │ port: 9092/29092 │     │ watermark +  ││
                     │ hours sched  │     │ topic:           │     │ dedup + DQ   ││
                     └──────────────┘     │ idx_sector_ticks │     └──────┬───────┘│
                                          └──────────────────┘            │       │
                                                                         ▼       │
                                                                    ┌──────────┐  │
                                                                    │   GCS    │  │
                                                                    │  Temp    │  │
                                                                    │  Staging │  │
                                                                    └────┬─────┘  │
                                                                         │       │
                              ┌──────────────────────────────────────────┘       │
                              ▼                                                     │
                    ┌──────────────────┐                                            │
                    │  Google BigQuery  │                                            │
                    │  top_sector_ticks │                                            │
                    └────────┬─────────┘                                            │
                             │                                                     │
                    ┌────────▼─────────┐                                            │
                    │  dbt Transform   │                                            │
                    │  (staging →  marts) │                                         │
                    └──────────────────┘                                            │
                                                                                    │
                              ┌─────────────────────────────────────────────────────┘
                              ▼
                    ┌──────────────────┐
                    │  Analyst/BI      │
                    │  Dashboard       │
                    └──────────────────┘
```

## 2. Komponen Pipeline

### 2.1 Producer (`producer.py`)

| Aspek | Detail |
|---|---|
| Fungsi | Fetch data intraday dari Yahoo Finance untuk 51 ticker IDX |
| Library | `yfinance`, `kafka-python`, `pandas` |
| Interval fetch | Market hours (Mon-Fri 09:00-15:00 WIB): `FETCH_INTERVAL_SECONDS` (default 60s) |
| | Off-hours: 3600s (1 jam) |
| Dedup | Cache `(ticker, timestamp)` — hanya kirim candle baru |
| Output | JSON ke Kafka topic `idx_sector_ticks` |
| Retry | 3× retry per ticker dengan exponential backoff |

**Payload JSON:**
```json
{
  "ticker": "BYAN.JK",
  "sector": "Energy",
  "timestamp": "2026-05-22T14:30:00+07:00",
  "fetch_ts": "2026-05-22T14:30:05+07:00",
  "open": 10000.0,
  "high": 10100.0,
  "low": 9900.0,
  "close": 10050.0,
  "volume": 1500000
}
```

### 2.2 Kafka Broker (`docker-compose.yml`)

| Aspek | Detail |
|---|---|
| Mode | KRaft (tanpa Zookeeper) |
| Image | `confluentinc/cp-kafka:7.5.5` |
| Internal port | 29092 (Spark) |
| External port | 9092 (Producer) |
| Topic | `idx_sector_ticks` (auto-created) |
| DLQ Topic | `idx_sector_ticks_dlq` (malformed JSON) |
| Partisi | 1 (single node dev) |

### 2.3 Spark Processor (`spark_processor.py`)

| Aspek | Detail |
|---|---|
| Fungsi | Baca stream dari Kafka → parse → clean → write ke BigQuery |
| Library | `pyspark`, `spark-bigquery-connector` |
| Mode | Structured Streaming, exactly-once |
| Starting offsets | `latest` (dengan checkpoint override) |
| Fail on data loss | `false` |

**Data cleaning:**
- JSON parsing dengan mode `PERMISSIVE` (field tak dikenal → NULL)
- NaN di kolom `open/high/low/close` → NULL
- Ticker null/empty → drop
- Timestamp ISO 8601 → Spark TimestampType
- Watermark 30 menit + `dropDuplicates(["ticker", "timestamp"])`

**Write strategy:**
- Valid data → BigQuery `top_sector_ticks` via `direct` write method
- Invalid data → Kafka DLQ topic `idx_sector_ticks_dlq`

### 2.4 BigQuery Table (`top_sector_ticks`)

**Schema:**

| Field | Type | Mode | Deskripsi |
|---|---|---|---|
| `ticker` | STRING | NULLABLE | Kode saham (contoh: `BYAN.JK`) |
| `sector` | STRING | NULLABLE | Sektor IDX |
| `timestamp` | TIMESTAMP | NULLABLE | Waktu candle (WIB) |
| `fetch_ts` | TIMESTAMP | NULLABLE | Waktu fetch sistem (WIB) |
| `open` | FLOAT | NULLABLE | Harga open |
| `high` | FLOAT | NULLABLE | Harga high |
| `low` | FLOAT | NULLABLE | Harga low |
| `close` | FLOAT | NULLABLE | Harga close |
| `volume` | INTEGER | NULLABLE | Volume transaksi |

**Partitioning:** `timestamp` — DAY
**Clustering:** `ticker`, `sector`

### 2.5 dbt Transformation Models

```
top_sector_ticks (raw)
        │
        ▼
stg_idx_sector_ticks (view — cast, filter 90 hari)
        │
        ├──▶ dim_ticker (table — dimensional)
        ├──▶ dim_sector (table — dimensional)
        │
        ▼
int_daily_stock_stats (table — partitioned, OHLCV harian per ticker)
        │
        ├──▶ int_sector_daily_summary (table — agregasi per sektor)
        ├──▶ int_ticker_rankings (view — ranking volume)
        │
        ▼
┌───────────────────┬───────────────────┬──────────────────┐
│                   │                   │                  │
▼                   ▼                   ▼                  ▼
mrt_top5_        mrt_sector_      mrt_market_       mrt_volume_
sector_daily     performance      breadth           anomalies
(table)          (table)          (table)           (table)
```

## 3. Data Flow Detail

### Step-by-step:

1. **Producer** fetch `period="1d", interval="1m"` dari Yahoo Finance untuk 51 ticker
2. Cek cache `_latest_ts_per_ticker` — skip candle yang sudah pernah dikirim
3. Kirim ke Kafka topic `idx_sector_ticks` dengan headers `schema_name` + `schema_version`
4. **Spark processor** baca stream dari Kafka
5. Parse JSON → split valid vs invalid
6. Data valid: NaN → NULL, filter null ticker, parse timestamp, dedup (watermark 30m)
7. Data valid → write ke BigQuery via GCS temp staging
8. Data invalid → write ke Kafka DLQ topic
9. **dbt** transformasi data staging → intermediate → marts

### Scheduling:

| Waktu | Interval | Behavior |
|---|---|---|
| Mon-Fri 09:00-15:00 WIB | 60s | Fetch aktif, kirim candle baru |
| Mon-Fri 15:00-09:00 WIB | 3600s | Cek 1 jam sekali (pasar tutup) |
| Sabtu-Minggu | 3600s | Cek 1 jam sekali (libur) |

## 4. Environment Variables

| Variable | Default | Required | Deskripsi |
|---|---|---|---|
| `GCP_PROJECT_ID` | — | ✅ | Project ID Google Cloud |
| `GOOGLE_APPLICATION_CREDENTIALS` | `./gcp-service-account.json` | ✅ | Path SA JSON |
| `GCP_BIGQUERY_DATASET` | — | ✅ | Nama dataset BigQuery |
| `GCP_BIGQUERY_TABLE` | — | ✅ | Nama table BigQuery |
| `GCS_TEMP_BUCKET` | — | ✅ | Bucket GCS untuk staging |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | ✅ | Alamat Kafka broker |
| `KAFKA_TOPIC` | `idx_sector_ticks` | ✅ | Kafka topic name |
| `SPARK_APP_NAME` | `RealTimeIDXProcessor` | — | Nama Spark app |
| `SPARK_CHECKPOINT_DIR` | `/tmp/spark-checkpoints/idx_stock` | — | Direktori checkpoint |
| `FETCH_INTERVAL_SECONDS` | `60` | — | Interval fetch (market hours) |
| `YFINANCE_DELAY_SECONDS` | `0.5` | — | Delay antar request yfinance |

## 5. 11 Sektor & 51 Ticker

| Sektor | Jumlah Ticker | Contoh |
|---|---|---|
| Energy | 5 | BYAN, DSSA, CUAN, AADI, BUMI |
| Basic Materials | 5 | TPIA, SMGR, INTP, SMBR, BRPT |
| Consumer Cyclicals | 5 | UNVR, ICBP, INDF, GGRM, HMSP |
| Consumer Non-Cyclicals | 5 | — |
| Healthcare | 5 | — |
| Properties & Real Estate | 5 | — |
| Technology | 5 | — |
| Infrastructures | 5 | — |
| Transportation & Logistic | 5 | — |
| Financials | 5 | BBCA, BBRI, BMRI, BBNI, DNET |
| Industrial | 5 | — |

Total: 51 ticker × ~270 candle/hari × 22 hari kerja = **~18.000 row/bulan** (dengan dedup)

## 6. Estimasi Biaya (Free Tier BigQuery)

| Item | Usage | Free Tier Limit |
|---|---|---|
| Storage | ~20 MB/bulan | 15 GB ✅ |
| Query | ~50-100 MB/bulan | 1 TB/bulan ✅ |
| Streaming | 0 (pake LOAD job) | — |

## 7. Schema Registry

**Avro schema:** `schemas/stock_tick_v1.avsc`

Producer kirim headers di setiap Kafka record:
- `schema_name: stock_tick`
- `schema_version: 1`

**Schema evolution rules:**
- Field baru harus nullable (`default: null`)
- Tidak boleh hapus field
- Tidak boleh ubah tipe field
- Spark processor pakai `PERMISSIVE` mode (forward compat)

## 8. Data Quality Checks

### In-stream (Spark):
- OHLCV logic (close between low and high)
- Volume non-negative
- Timestamp freshness (< 1 jam dari real-time)
- Null critical fields (ticker, timestamp)
- Sector coverage (semua 11 sektor)

### Post-load (dbt):
- `not_null` pada kolom kritis
- `accepted_values` untuk sector
- `expression_is_true` untuk `close > 0`, `volume >= 0`

## 9. CI/CD Pipeline (GitHub Actions)

| Workflow | Trigger | Jobs |
|---|---|---|
| CI | Push/PR ke `master` | Lint (ruff) → Test Producer → Test Spark |
| CD | Tag `v*.*.*` atau manual | Build & Push Docker images ke GHCR |

## 10. Monitoring & Observability

| Komponen | Tool | Port |
|---|---|---|
| Spark Metrics | Prometheus (config) | — |
| Dashboard | Grafana (provisioned) | 3000 |
| Pipeline health | Airflow DAG (`idx_streaming_monitor`) | — |
| Container health | Docker healthcheck | — |

## 11. Secrets Management

| Secret | Storage Lokal | Storage Production |
|---|---|---|
| GCP Service Account | `gcp-service-account.json` (gitignored) | GCP Secret Manager |
| Konfigurasi | `.env` (gitignored) | GCP Secret Manager |
| Grafana password | `.env` | GCP Secret Manager |
| Airflow password | `.env` | GCP Secret Manager |

Fallback: `utils/secrets.py` — coba Secret Manager dulu, fallback ke `.env`
