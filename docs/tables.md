# BigQuery Dataset: `idx_stock_data` — Table Reference

Dataset location: `asia-southeast2` (Jakarta)

## Table List

| No | Table | Type | Layer | Baris | Ukuran |
|---|---|---|---|---|---|
| 1 | `top_sector_ticks` | TABLE | Raw (source) | ~20.000 | ~2 MB |
| 2 | `stg_idx_sector_ticks` | VIEW | Staging | — | — |
| 3 | `dim_ticker` | TABLE | Dimensional | ~53 | ~2 KB |
| 4 | `dim_sector` | TABLE | Dimensional | ~11 | ~500 B |
| 5 | `fct_intraday_ticks` | TABLE | Fact | ~20.000 | ~2 MB |
| 6 | `int_daily_stock_stats` | TABLE | Intermediate | ~1.200 | ~200 KB |
| 7 | `int_sector_daily_summary` | TABLE | Intermediate | ~25 | ~5 KB |
| 8 | `mrt_top5_sector_daily` | TABLE | Mart | ~55 | ~10 KB |
| 9 | `mrt_sector_performance` | TABLE | Mart | ~11 | ~2 KB |
| 10 | `mrt_market_breadth` | TABLE | Mart | ~1 | ~500 B |
| 11 | `mrt_volume_anomalies` | TABLE | Mart | ~0 | ~0 KB |

---

## 1. `top_sector_ticks` (Raw Source)

**Deskripsi:** Tabel utama yang ditulis langsung oleh pipeline Spark Streaming. Berisi data intraday 1-menit dari 51 ticker IDX (Top 5 per sektor).

**Source:** Spark processor (`spark_processor.py`) → `BigQueryStreamingSink` → `WRITE_APPEND`

**Partitioning:** `timestamp` (DAY)
**Clustering:** `ticker`, `sector`

### Schema

| Field | Type | Mode | Deskripsi | Contoh |
|---|---|---|---|---|
| `ticker` | STRING | NULLABLE | Kode saham IDX | `BYAN.JK` |
| `sector` | STRING | NULLABLE | Nama sektor IDX | `Energy` |
| `timestamp` | TIMESTAMP | NULLABLE | Waktu candle (WIB) | `2026-05-25 14:30:00+07:00` |
| `fetch_ts` | TIMESTAMP | NULLABLE | Waktu sistem fetch (WIB) | `2026-05-25 14:30:05+07:00` |
| `open` | FLOAT | NULLABLE | Harga pembuka candle | `10000.0` |
| `high` | FLOAT | NULLABLE | Harga tertinggi candle | `10100.0` |
| `low` | FLOAT | NULLABLE | Harga terendah candle | `9900.0` |
| `close` | FLOAT | NULLABLE | Harga penutup candle | `10050.0` |
| `volume` | INTEGER | NULLABLE | Volume transaksi | `1500000` |

### Sample Data

```sql
SELECT * FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks`
WHERE ticker = 'BBCA.JK'
  AND DATE(timestamp) = '2026-05-25'
ORDER BY timestamp DESC
LIMIT 5;
```

### Data Pipeline Flow

```
yfinance → Producer (dedup) → Kafka → Spark (watermark + dedup + clean) → BigQuery
```

### Update Frequency

| Waktu | Interval |
|---|---|
| Market hours (Mon-Fri 09:00-15:00 WIB) | ~60 detik (batch) |
| Off-hours | ~1 jam |
| Candle per batch | ~0-50 (hanya candle baru) |

---

## 2. `stg_idx_sector_ticks` (Staging View)

**Deskripsi:** View yang membersihkan dan memfilter raw data. Hanya menyajikan data 90 hari terakhir dengan kolom tambahan untuk analisis.

**Type:** VIEW (tidak menyimpan data, query langsung dari `top_sector_ticks`)

**Materialization:** `view` (selalu fresh dari source)

### SQL Definition

```sql
SELECT
  ticker, sector, timestamp, fetch_ts,
  open, high, low, close, volume,
  DATE(timestamp) AS date_id,
  TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket
FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
```

### Additional Columns

| Kolom | Type | Asal |
|---|---|---|
| `date_id` | DATE | Ekstrak dari `timestamp` |
| `hour_bucket` | TIMESTAMP | Trunc ke jam |

### Data Quality Tests

- `not_null`: ticker, sector, timestamp, close, volume, date_id
- `accepted_values`: sector (harus match 11 sektor IDX)
- `expression_is_true`: close > 0, volume >= 0

---

## 3. `dim_ticker` (Dimensional)

**Deskripsi:** Tabel dimensi yang berisi daftar unik ticker saham dengan metadata.

**Materialization:** `table` (daily refresh)

### Schema

| Field | Type | Deskripsi |
|---|---|---|
| `ticker_sk` | INTEGER | Surrogate key (primary key dimensi) |
| `ticker_code` | STRING | Kode saham (contoh: `BYAN.JK`) |
| `ticker_name` | STRING | Nama saham (tanpa `.JK`, capital case) |
| `sector` | STRING | Sektor asal |
| `first_seen_date` | DATE | Pertama kali terdeteksi di pipeline |
| `last_seen_date` | DATE | Terakhir kali terdeteksi |
| `is_active` | BOOLEAN | Masih aktif? (last_seen < 7 hari) |

### Sample Query

```sql
SELECT * FROM `idx-analytics-platform.idx_stock_data.dim_ticker`
WHERE is_active = TRUE
ORDER BY sector, ticker_code;
```

---

## 4. `dim_sector` (Dimensional)

**Deskripsi:** Tabel dimensi yang berisi daftar 11 sektor IDX + kategorisasi.

**Materialization:** `table` (weekly refresh)

### Schema

| Field | Type | Deskripsi |
|---|---|---|
| `sector_sk` | INTEGER | Surrogate key |
| `sector_name` | STRING | Nama sektor IDX |
| `sector_category` | STRING | Kategori sektor (Financial, Commodities, etc) |

### Sector Categories

| Sector | Category |
|---|---|
| Financials, Properties & Real Estate | Financial |
| Technology, Infrastructures, Transportation & Logistic, Industrial | Infrastructure & Tech |
| Energy, Basic Materials | Commodities |
| Consumer Cyclicals, Consumer Non-Cyclicals, Healthcare | Consumer & Services |

### Sample Query

```sql
SELECT * FROM `idx-analytics-platform.idx_stock_data.dim_sector`
ORDER BY sector_category, sector_name;
```

---

## 5. `fct_intraday_ticks` (Fact)

**Deskripsi:** Fact table dengan surrogate keys dari `dim_ticker` dan `dim_sector`. Siap untuk join dimensional.

**Partitioning:** `timestamp` (DAY)
**Clustering:** `ticker_sk`, `sector_sk`

**Materialization:** `table` (hourly incremental)

### Schema

| Field | Type | Deskripsi |
|---|---|---|
| `ticker_sk` | INTEGER | Foreign key ke `dim_ticker.ticker_sk` |
| `sector_sk` | INTEGER | Foreign key ke `dim_sector.sector_sk` |
| `timestamp` | TIMESTAMP | Waktu candle |
| `open` | FLOAT | Harga open |
| `high` | FLOAT | Harga high |
| `low` | FLOAT | Harga low |
| `close` | FLOAT | Harga close |
| `volume` | INTEGER | Volume |
| `fetch_ts` | TIMESTAMP | Waktu fetch |

### Sample Query (Join Dimensional)

```sql
SELECT
  t.ticker_name,
  s.sector_name,
  f.close,
  f.volume
FROM `idx-analytics-platform.idx_stock_data.fct_intraday_ticks` f
JOIN `idx-analytics-platform.idx_stock_data.dim_ticker` t USING (ticker_sk)
JOIN `idx-analytics-platform.idx_stock_data.dim_sector` s USING (sector_sk)
WHERE DATE(f.timestamp) = CURRENT_DATE()
ORDER BY f.volume DESC;
```

---

## 6. `int_daily_stock_stats` (Intermediate)

**Deskripsi:** Agregasi harian per ticker. Menyediakan statistik OHLCV harian, VWAP, dan persentase perubahan harga.

**Partitioning:** `date_id` (DAY)
**Clustering:** `ticker`

**Materialization:** `table` (daily)

### Schema

| Field | Type | Deskripsi | Rumus |
|---|---|---|---|
| `ticker` | STRING | Kode saham | — |
| `sector` | STRING | Sektor | — |
| `date_id` | DATE | Tanggal | — |
| `first_tick_ts` | TIMESTAMP | Tick pertama hari ini | — |
| `last_tick_ts` | TIMESTAMP | Tick terakhir hari ini | — |
| `open_price` | FLOAT | Harga open pertama | `FIRST(open ORDER BY timestamp)` |
| `close_price` | FLOAT | Harga close terakhir | `LAST(close ORDER BY timestamp)` |
| `high_price` | FLOAT | Harga tertinggi hari ini | `MAX(high)` |
| `low_price` | FLOAT | Harga terendah hari ini | `MIN(low)` |
| `total_volume` | INTEGER | Total volume harian | `SUM(volume)` |
| `tick_count` | INTEGER | Jumlah candle intraday | `COUNT(*)` |
| `vwap` | FLOAT | Volume-weighted avg price | `SUM(close*volume)/SUM(volume)` |
| `price_change_pct` | FLOAT | Perubahan harga (%) | `(close-open)/open` |

### Sample Query

```sql
-- Top 5 gainers hari ini
SELECT ticker, sector, price_change_pct, total_volume
FROM `idx-analytics-platform.idx_stock_data.int_daily_stock_stats`
WHERE date_id = CURRENT_DATE()
ORDER BY price_change_pct DESC
LIMIT 10;
```

---

## 7. `int_sector_daily_summary` (Intermediate)

**Deskripsi:** Ringkasan harian per sektor. Menyediakan metrik agregat untuk perbandingan antar sektor.

**Partitioning:** `date_id` (DAY)

**Materialization:** `table` (daily)

### Schema

| Field | Type | Deskripsi |
|---|---|---|
| `sector` | STRING | Nama sektor |
| `date_id` | DATE | Tanggal |
| `ticker_count` | INTEGER | Jumlah ticker aktif |
| `sector_total_volume` | INTEGER | Total volume sektor |
| `avg_price_change_pct` | FLOAT | Rata-rata perubahan harga |
| `tickers_up` | INTEGER | Ticker yang naik |
| `tickers_down` | INTEGER | Ticker yang turun |
| `tickers_flat` | INTEGER | Ticker yang flat |
| `sector_performance_rank` | INTEGER | Ranking performa sektor (1=terbaik) |

### Sample Query

```sql
-- Sektor terbaik hari ini
SELECT sector, avg_price_change_pct, tickers_up, tickers_down
FROM `idx-analytics-platform.idx_stock_data.int_sector_daily_summary`
WHERE date_id = CURRENT_DATE()
ORDER BY sector_performance_rank;
```

---

## 8. `mrt_top5_sector_daily` (Mart)

**Deskripsi:** **Top 5 saham per sektor berdasarkan volume.** Ini adalah output utama pipeline — menjawab pertanyaan "Siapa Top 5 saham paling likuid di setiap sektor hari ini?"

**Partitioning:** `date_id` (DAY)

**Materialization:** `table` (daily)

### Schema

| Field | Type | Deskripsi |
|---|---|---|
| `ticker` | STRING | Kode saham |
| `sector` | STRING | Sektor |
| `date_id` | DATE | Tanggal |
| `open_price` | FLOAT | Harga open |
| `close_price` | FLOAT | Harga close |
| `high_price` | FLOAT | Harga high |
| `low_price` | FLOAT | Harga low |
| `total_volume` | INTEGER | Volume harian |
| `vwap` | FLOAT | VWAP |
| `price_change_pct` | FLOAT | Perubahan harga |
| `volume_rank` | INTEGER | Peringkat volume dalam sektor (1-5) |

### Sample Query

```sql
-- Top 5 saham per sektor hari ini
SELECT sector, ticker, volume_rank, close_price, total_volume
FROM `idx-analytics-platform.idx_stock_data.mrt_top5_sector_daily`
WHERE date_id = CURRENT_DATE()
ORDER BY sector, volume_rank;
```

---

## 9. `mrt_sector_performance` (Mart)

**Deskripsi:** Ranking performa sektor hari ini dengan metrik advance/decline ratio. Siap untuk sector heatmap dashboard.

**Partitioning:** `date_id` (DAY)

**Materialization:** `table` (daily)

### Schema

| Field | Type | Deskripsi |
|---|---|---|
| `sector_sk` | INTEGER | Foreign key ke `dim_sector` |
| `sector` | STRING | Nama sektor |
| `date_id` | DATE | Tanggal |
| `avg_price_change_pct` | FLOAT | Rata-rata perubahan |
| `sector_total_volume` | INTEGER | Total volume |
| `ticker_count` | INTEGER | Jumlah ticker |
| `tickers_up` | INTEGER | Ticker naik |
| `tickers_down` | INTEGER | Ticker turun |
| `tickers_flat` | INTEGER | Ticker flat |
| `advance_decline_ratio` | FLOAT | Rasio naik/turun |
| `sector_performance_rank` | INTEGER | Ranking performa |

---

## 10. `mrt_market_breadth` (Mart)

**Deskripsi:** Metrik kesehatan pasar secara keseluruhan — berapa saham naik vs turun, total volume pasar, rata-rata perubahan.

**Partitioning:** `date_id` (DAY)

**Materialization:** `table` (daily)

### Schema

| Field | Type | Deskripsi |
|---|---|---|
| `date_id` | DATE | Tanggal |
| `total_tickers` | INTEGER | Total ticker dengan data |
| `gainers` | INTEGER | Ticker naik |
| `losers` | INTEGER | Ticker turun |
| `flat` | INTEGER | Ticker flat |
| `avg_change_pct` | FLOAT | Rata-rata perubahan pasar |
| `total_volume` | INTEGER | Total volume seluruh pasar |
| `advance_decline_ratio` | FLOAT | Rasio gainers/losers |

### Sample Query

```sql
-- Market health hari ini
SELECT * FROM `idx-analytics-platform.idx_stock_data.mrt_market_breadth`
WHERE date_id = CURRENT_DATE();
```

---

## 11. `mrt_volume_anomalies` (Mart)

**Deskripsi:** Mendeteksi anomali volume — ticker dengan volume hari ini > 2 standar deviasi dari rata-rata 7 hari terakhir. Berguna untuk alerting.

**Materialization:** `table` (daily, bisa kosong jika tidak ada anomali)

### Schema

| Field | Type | Deskripsi |
|---|---|---|
| `ticker` | STRING | Kode saham |
| `sector` | STRING | Sektor |
| `date_id` | DATE | Tanggal anomali |
| `total_volume` | INTEGER | Volume hari ini |
| `avg_volume_7d` | FLOAT | Rata-rata volume 7 hari |
| `stddev_volume_7d` | FLOAT | Standar deviasi 7 hari |
| `volume_z_score` | FLOAT | Z-score volume |

### Interpretation

| Z-Score | Arti |
|---|---|
| 2.0 - 3.0 | Volume tinggi (warning) |
| 3.0 - 5.0 | Volume sangat tinggi (critical) |
| > 5.0 | Volume abnormal (investigasi) |

### Sample Query

```sql
-- Cek anomali volume
SELECT ticker, sector, total_volume, avg_volume_7d, volume_z_score
FROM `idx-analytics-platform.idx_stock_data.mrt_volume_anomalies`
ORDER BY volume_z_score DESC;
```

---

## Entity Relationship Diagram (ERD)

```
┌───────────────────┐       ┌───────────────────────┐
│    dim_ticker     │       │   dim_sector          │
├───────────────────┤       ├───────────────────────┤
│ ticker_sk (PK)    │◄────┐ │ sector_sk (PK)        │◄────┐
│ ticker_code       │     │ │ sector_name           │     │
│ ticker_name       │     │ │ sector_category       │     │
│ sector            │     │ └───────────────────────┘     │
│ first_seen_date   │     │                               │
│ last_seen_date    │     │                               │
│ is_active         │     │                               │
└───────────────────┘     │                               │
                          │                               │
┌─────────────────────────┴───────────────────────────────┴──┐
│                  fct_intraday_ticks                         │
├─────────────────────────────────────────────────────────────┤
│ ticker_sk (FK) ────────────────────────────────────────────┘
│ sector_sk (FK) ────────────────────────────────────────────┘
│ timestamp, open, high, low, close, volume, fetch_ts         │
└─────────────────────────────┬───────────────────────────────┘
                              │
                    ┌─────────┴──────────┐
                    │                    │
                    ▼                    ▼
       int_daily_stock_stats    (aggregates harian)
                    │
          ┌─────────┼─────────┐
          │         │         │
          ▼         ▼         ▼
   mrt_top5_    mrt_sector_   mrt_market_     mrt_volume_
   sector_daily performance  breadth          anomalies
```

## Data Lineage

```
top_sector_ticks (raw)
       │
       ▼
stg_idx_sector_ticks (view)
       │
       ├──▶ dim_ticker (table)
       ├──▶ dim_sector (table)
       │
       ▼
fct_intraday_ticks (table)
       │
       ▼
int_daily_stock_stats (table)
       │
       ├──▶ int_sector_daily_summary (table)
       │
       ▼
┌──────────┬──────────┬──────────┬──────────────┐
│          │          │          │              │
▼          ▼          ▼          ▼              ▼
mrt_top5_  mrt_sector_ mrt_market_ mrt_volume_
sector_daily performance breadth   anomalies
```
