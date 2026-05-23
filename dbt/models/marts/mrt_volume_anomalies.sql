WITH daily_stats AS (
    SELECT * FROM {{ ref('int_daily_stock_stats') }}
),

stats AS (
    SELECT
        ticker,
        AVG(total_volume) AS avg_volume_7d,
        STDDEV(total_volume) AS stddev_volume_7d
    FROM daily_stats
    WHERE date_id >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY ticker
),

recent AS (
    SELECT *
    FROM daily_stats
    WHERE date_id = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)

SELECT
    r.ticker,
    r.sector,
    r.date_id,
    r.total_volume,
    s.avg_volume_7d,
    s.stddev_volume_7d,
    SAFE_DIVIDE(r.total_volume - s.avg_volume_7d, NULLIF(s.stddev_volume_7d, 0)) AS volume_z_score
FROM recent r
JOIN stats s USING (ticker)
WHERE s.stddev_volume_7d > 0
  AND SAFE_DIVIDE(r.total_volume - s.avg_volume_7d, s.stddev_volume_7d) > 2.0
ORDER BY volume_z_score DESC
