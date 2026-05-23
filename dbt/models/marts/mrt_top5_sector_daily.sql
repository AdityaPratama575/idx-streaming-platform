WITH ranked AS (
    SELECT * FROM {{ ref('int_ticker_rankings') }}
)

SELECT
    sector,
    date_id,
    ticker,
    open_price,
    close_price,
    high_price,
    low_price,
    total_volume,
    vwap,
    price_change_pct,
    volume_rank
FROM ranked
WHERE volume_rank <= 5
ORDER BY sector, volume_rank
