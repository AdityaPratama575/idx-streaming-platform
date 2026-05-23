#!/usr/bin/env python3
"""Create all dbt model tables directly via BigQuery API (bypass dbt CLI)."""
import os, sys
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-service-account.json"
client = bigquery.Client()

PROJECT = client.project
DATASET = "idx_stock_data"

models = {}

# ── Staging ──
models["stg_idx_sector_ticks"] = f"""
CREATE OR REPLACE VIEW `{PROJECT}.{DATASET}.stg_idx_sector_ticks` AS
SELECT ticker, sector, timestamp, fetch_ts, open, high, low, close, volume,
       DATE(timestamp) AS date_id,
       TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket
FROM `{PROJECT}.{DATASET}.top_sector_ticks`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
"""

# ── Dimensional ──
models["dim_ticker"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.dim_ticker` CLUSTER BY ticker_code AS
WITH src AS (
    SELECT DISTINCT ticker, sector,
           MIN(DATE(timestamp)) AS first_seen, MAX(DATE(timestamp)) AS last_seen
    FROM `{PROJECT}.{DATASET}.top_sector_ticks` GROUP BY ticker, sector
)
SELECT ROW_NUMBER() OVER (ORDER BY ticker) AS ticker_sk,
       ticker AS ticker_code,
       INITCAP(REPLACE(ticker, '.JK', '')) AS ticker_name,
       sector, first_seen AS first_seen_date, last_seen AS last_seen_date,
       last_seen >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS is_active
FROM src
"""

models["dim_sector"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.dim_sector` AS
SELECT ROW_NUMBER() OVER (ORDER BY sector) AS sector_sk, sector AS sector_name,
       CASE WHEN sector IN ('Financials','Properties & Real Estate') THEN 'Financial'
            WHEN sector IN ('Technology','Infrastructures','Transportation & Logistic') THEN 'Infrastructure & Tech'
            WHEN sector IN ('Energy','Basic Materials') THEN 'Commodities'
            ELSE 'Consumer & Services' END AS sector_category
FROM (SELECT DISTINCT sector FROM `{PROJECT}.{DATASET}.top_sector_ticks`)
"""

# ── Fact ──
models["fct_intraday_ticks"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.fct_intraday_ticks`
PARTITION BY DATE(timestamp) CLUSTER BY ticker_sk AS
SELECT d.ticker_sk, s.sector_sk, t.timestamp,
       t.open, t.high, t.low, t.close, t.volume, t.fetch_ts
FROM `{PROJECT}.{DATASET}.top_sector_ticks` t
LEFT JOIN `{PROJECT}.{DATASET}.dim_ticker` d ON t.ticker = d.ticker_code
LEFT JOIN `{PROJECT}.{DATASET}.dim_sector` s ON t.sector = s.sector_name
"""

# ── Intermediate ──
models["int_daily_stock_stats"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.int_daily_stock_stats`
PARTITION BY date_id CLUSTER BY ticker AS
WITH grouped AS (
    SELECT ticker, sector, DATE(timestamp) AS date_id,
           ARRAY_AGG(open ORDER BY timestamp LIMIT 1)[OFFSET(0)] AS open_price,
           ARRAY_AGG(close ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS close_price,
           MAX(high) AS high_price, MIN(low) AS low_price,
           SUM(volume) AS total_volume, COUNT(*) AS tick_count
    FROM `{PROJECT}.{DATASET}.top_sector_ticks`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    GROUP BY ticker, sector, DATE(timestamp)
)
SELECT *, SAFE_DIVIDE(close_price - open_price, open_price) AS price_change_pct FROM grouped
"""

models["int_sector_daily_summary"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.int_sector_daily_summary` PARTITION BY date_id AS
SELECT sector, date_id, COUNT(DISTINCT ticker) AS ticker_count,
       SUM(total_volume) AS sector_total_volume,
       AVG(price_change_pct) AS avg_price_change_pct,
       COUNTIF(price_change_pct > 0) AS tickers_up,
       COUNTIF(price_change_pct < 0) AS tickers_down,
       COUNTIF(ABS(price_change_pct) < 0.001) AS tickers_flat,
       ROW_NUMBER() OVER (PARTITION BY date_id ORDER BY AVG(price_change_pct) DESC) AS sector_performance_rank
FROM `{PROJECT}.{DATASET}.int_daily_stock_stats`
GROUP BY sector, date_id
"""

# ── Marts ──
models["mrt_top5_sector_daily"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.mrt_top5_sector_daily` PARTITION BY date_id AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY sector, date_id ORDER BY total_volume DESC) AS volume_rank
    FROM `{PROJECT}.{DATASET}.int_daily_stock_stats`
)
SELECT * FROM ranked WHERE volume_rank <= 5
"""

models["mrt_sector_performance"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.mrt_sector_performance` PARTITION BY date_id AS
SELECT s.sector_sk, sum.sector, sum.date_id, sum.avg_price_change_pct,
       sum.sector_total_volume, sum.ticker_count,
       sum.tickers_up, sum.tickers_down, sum.tickers_flat,
       SAFE_DIVIDE(sum.tickers_up, NULLIF(sum.tickers_down, 0)) AS advance_decline_ratio,
       sum.sector_performance_rank
FROM `{PROJECT}.{DATASET}.int_sector_daily_summary` sum
LEFT JOIN `{PROJECT}.{DATASET}.dim_sector` s ON sum.sector = s.sector_name
"""

models["mrt_market_breadth"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.mrt_market_breadth` PARTITION BY date_id AS
SELECT date_id, COUNT(DISTINCT ticker) AS total_tickers,
       COUNTIF(price_change_pct > 0) AS gainers,
       COUNTIF(price_change_pct < 0) AS losers,
       COUNTIF(ABS(price_change_pct) < 0.001) AS flat,
       AVG(price_change_pct) AS avg_change_pct, SUM(total_volume) AS total_volume
FROM `{PROJECT}.{DATASET}.int_daily_stock_stats`
GROUP BY date_id
"""

models["mrt_volume_anomalies"] = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.mrt_volume_anomalies` PARTITION BY date_id AS
WITH stats AS (
    SELECT ticker, AVG(total_volume) AS avg_7d, STDDEV(total_volume) AS std_7d
    FROM `{PROJECT}.{DATASET}.int_daily_stock_stats`
    WHERE date_id >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY ticker
), today AS (
    SELECT * FROM `{PROJECT}.{DATASET}.int_daily_stock_stats`
    WHERE date_id = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
SELECT r.ticker, r.sector, r.date_id, r.total_volume,
       s.avg_7d, s.std_7d,
       SAFE_DIVIDE(r.total_volume - s.avg_7d, NULLIF(s.std_7d, 0)) AS volume_z_score
FROM today r JOIN stats s USING (ticker)
WHERE s.std_7d > 0 AND SAFE_DIVIDE(r.total_volume - s.avg_7d, s.std_7d) > 2.0
"""

ok = 0
fail = 0
for name, sql in models.items():
    print(f"Creating {name}... ", end="", flush=True)
    try:
        client.query(sql).result()
        print("OK")
        ok += 1
    except Exception as e:
        print(f"ERROR: {e}")
        fail += 1

print(f"\nDone: {ok} succeeded, {fail} failed")
sys.exit(1 if fail else 0)
