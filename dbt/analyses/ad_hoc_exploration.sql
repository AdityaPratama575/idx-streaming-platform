WITH top5 AS (
    SELECT * FROM {{ ref('mrt_top5_sector_daily') }}
)

SELECT
    sector,
    date_id,
    ticker,
    volume_rank,
    ROUND(close_price, 0) AS close_price,
    ROUND(price_change_pct * 100, 2) AS change_pct,
    total_volume
FROM top5
WHERE date_id = CURRENT_DATE()
ORDER BY sector, volume_rank
