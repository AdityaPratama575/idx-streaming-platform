-- Data Quality Checks (run manually or via Airflow/script)
-- These queries detect anomalies in the raw tick data.

-- Check 1: OHLCV Logic — Close must be between Low and High
SELECT
    'ohlcv_logic' AS check_id,
    COUNT(*) AS failed_rows,
    COUNT(*) / NULLIF((SELECT COUNT(*) FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks` WHERE DATE(timestamp) = CURRENT_DATE()), 0) AS failure_rate
FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks`
WHERE DATE(timestamp) = CURRENT_DATE()
  AND (close < low * 0.999 OR close > high * 1.001);

-- Check 2: Negative Volume
SELECT
    'negative_volume' AS check_id,
    COUNT(*) AS failed_rows
FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks`
WHERE DATE(timestamp) = CURRENT_DATE()
  AND volume < 0;

-- Check 3: Null Critical Fields
SELECT
    'null_critical_fields' AS check_id,
    COUNT(*) AS failed_rows
FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks`
WHERE DATE(timestamp) = CURRENT_DATE()
  AND (ticker IS NULL OR timestamp IS NULL);

-- Check 4: Stale Data (> 1 hour from now)
SELECT
    'stale_data' AS check_id,
    COUNT(*) AS failed_rows
FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks`
WHERE DATE(timestamp) = CURRENT_DATE()
  AND timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
