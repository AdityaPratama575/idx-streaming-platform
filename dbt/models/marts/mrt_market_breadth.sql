WITH daily_stats AS (
    SELECT * FROM {{ ref('int_daily_stock_stats') }}
)

SELECT
    date_id,
    COUNT(DISTINCT ticker) AS total_tickers,
    COUNTIF(price_change_pct > 0) AS gainers,
    COUNTIF(price_change_pct < 0) AS losers,
    COUNTIF(ABS(price_change_pct) < 0.001) AS flat,
    AVG(price_change_pct) AS avg_change_pct,
    SUM(total_volume) AS total_volume,
    SAFE_DIVIDE(COUNTIF(price_change_pct > 0), NULLIF(COUNTIF(price_change_pct < 0), 0)) AS advance_decline_ratio
FROM daily_stats
GROUP BY date_id
ORDER BY date_id DESC
