# Issue 18: Data Modeling — BigQuery Aggregate Tables

## Tujuan
Membangun data model dimensional dan aggregate tables di BigQuery untuk analytics, di luar raw table `top_sector_ticks`. Model ini akan menjadi foundation untuk dashboard dan analyst queries.

---

## Spesifikasi

### A. Entity Relationship Diagram (Conceptual)

```
┌──────────────────┐     ┌──────────────────────┐     ┌───────────────────┐
│   dim_ticker     │     │  fct_intraday_ticks  │     │   dim_sector      │
├──────────────────┤     ├──────────────────────┤     ├───────────────────┤
│ ticker_sk (PK)   │◄────│ ticker_sk (FK)       │────►│ sector_sk (PK)    │
│ ticker_code      │     │ sector_sk (FK)       │     │ sector_name       │
│ ticker_name      │     │ timestamp            │     │ sector_category   │
│ sector_sk (FK)   │     │ open, high, low,     │     │ sub_sector        │
│ listing_date     │     │ close, volume        │     │ market_cap_rank   │
│ is_active        │     │ fetch_ts             │     └───────────────────┘
└──────────────────┘     │ data_source          │
                         └──────────────────────┘
                                  │
                                  │ aggregates to:
                                  ▼
┌──────────────────────┐  ┌──────────────────────────┐
│ agg_daily_ticker     │  │ agg_sector_hourly        │
├──────────────────────┤  ├──────────────────────────┤
│ ticker_sk            │  │ sector_sk                │
│ date_id              │  │ hour_bucket              │
│ open, close          │  │ avg_price_change_pct     │
│ high, low            │  │ total_volume             │
│ volume               │  │ tickers_up / tickers_down│
│ price_change_pct     │  │ top_gainer_ticker        │
│ vwap                 │  │ top_loser_ticker         │
│ volume_rank_sector   │  └──────────────────────────┘
└──────────────────────┘
```

### B. Dimensional Tables (via dbt — lihat Issue 12)

#### `dim_ticker`

```sql
CREATE OR REPLACE TABLE dim_ticker AS
SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY ticker) AS ticker_sk,
  ticker                              AS ticker_code,
  INITCAP(REPLACE(ticker, '.JK', '')) AS ticker_name,
  sector,
  MIN(DATE(timestamp))                AS first_seen_date,
  MAX(DATE(timestamp))                AS last_seen_date,
  TRUE                                AS is_active
FROM top_sector_ticks
GROUP BY ticker, sector;
```

#### `dim_sector`

```sql
CREATE OR REPLACE TABLE dim_sector AS
SELECT
  ROW_NUMBER() OVER (ORDER BY sector) AS sector_sk,
  sector                              AS sector_name,
  CASE
    WHEN sector IN ('Financials', 'Properties & Real Estate') THEN 'Financial'
    WHEN sector IN ('Technology', 'Infrastructures', 'Transportation & Logistic') THEN 'Infrastructure & Tech'
    WHEN sector IN ('Energy', 'Basic Materials') THEN 'Commodities'
    ELSE 'Consumer & Services'
  END AS sector_category
FROM (SELECT DISTINCT sector FROM top_sector_ticks);
```

### C. Fact Table

#### `fct_intraday_ticks`

Partitioned & clustered version of raw `top_sector_ticks`:

```sql
CREATE OR REPLACE TABLE fct_intraday_ticks
PARTITION BY DATE(timestamp)
CLUSTER BY ticker, sector
AS
SELECT
  d.ticker_sk,
  s.sector_sk,
  t.timestamp,
  t.open, t.high, t.low, t.close, t.volume,
  t.fetch_ts
FROM top_sector_ticks t
LEFT JOIN dim_ticker d ON t.ticker = d.ticker_code
LEFT JOIN dim_sector s ON t.sector = s.sector_name;
```

### D. Aggregate Tables

#### `agg_daily_ticker`

```sql
-- Metric harian per ticker
SELECT
  ticker_sk,
  DATE(timestamp) AS date_id,
  -- OHLCV aggregation
  FIRST_VALUE(open)   OVER (PARTITION BY ticker_sk, DATE(timestamp) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS open_price,
  LAST_VALUE(close)   OVER (...) AS close_price,
  MAX(high)           OVER (...) AS high_price,
  MIN(low)            OVER (...) AS low_price,
  SUM(volume)         OVER (...) AS total_volume,
  -- Calculated metrics
  SAFE_DIVIDE(close_price - open_price, open_price) AS price_change_pct,
  SAFE_DIVIDE(SUM(close * volume), SUM(volume))     AS vwap,
  ROW_NUMBER() OVER (PARTITION BY sector, DATE(timestamp) ORDER BY SUM(volume) DESC) AS volume_rank_in_sector
FROM fct_intraday_ticks
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);
```

#### `agg_sector_hourly`

```sql
-- Metric per jam per sektor
SELECT
  sector_sk,
  TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
  AVG(price_change_pct)            AS avg_price_change_pct,
  SUM(volume)                      AS total_volume,
  COUNTIF(price_change_pct > 0)    AS tickers_up,
  COUNTIF(price_change_pct < 0)    AS tickers_down,
  COUNTIF(ABS(price_change_pct) < 0.001) AS tickers_flat
FROM agg_daily_ticker
GROUP BY sector_sk, hour_bucket;
```

### E. SQL Views untuk Dashboard (pre-built)

#### `view_top5_sector_by_volume`

```sql
-- Top 5 saham per sektor by volume (dashboard-ready)
SELECT sector_name, ticker_code, total_volume, volume_rank_in_sector
FROM agg_daily_ticker a
JOIN dim_ticker d ON a.ticker_sk = d.ticker_sk
JOIN dim_sector s ON d.sector_sk = s.sector_sk
WHERE volume_rank_in_sector <= 5
  AND date_id = CURRENT_DATE();
```

#### `view_market_summary`

```sql
SELECT
  COUNT(DISTINCT ticker_sk) AS total_tickers,
  AVG(price_change_pct)     AS avg_market_change,
  COUNTIF(price_change_pct > 0) AS gainers,
  COUNTIF(price_change_pct < 0) AS losers,
  SUM(total_volume)         AS total_market_volume
FROM agg_daily_ticker
WHERE date_id = CURRENT_DATE();
```

### F. Materialization Schedule (via dbt)

| Table | Refresh | Method |
|---|---|---|
| `dim_ticker` | Daily | `table` (full refresh) |
| `dim_sector` | Weekly | `table` (full refresh) |
| `fct_intraday_ticks` | Hourly | Incremental append |
| `agg_daily_ticker` | Daily | `table` |
| `agg_sector_hourly` | Hourly | Incremental merge |

---

## Acceptance Criteria

- [ ] Semua tabel dimensional (`dim_ticker`, `dim_sector`) terbuat di BigQuery
- [ ] Fact table `fct_intraday_ticks` terisi dengan surrogate keys
- [ ] Aggregate tables menghasilkan data yang konsisten
- [ ] Query `view_top5_sector_by_volume` menghasilkan output yang match dengan producer input
- [ ] All tables partitioned by date, clustered by relevant columns
- [ ] Tidak ada data duplication di aggregate tables
- [ ] ERD diagram didokumentasikan di `docs/data_model.md`
