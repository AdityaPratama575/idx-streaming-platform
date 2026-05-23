WITH daily_stats AS (
    SELECT * FROM {{ ref('int_daily_stock_stats') }}
),

sector_agg AS (
    SELECT
        sector,
        date_id,
        COUNT(DISTINCT ticker) AS ticker_count,
        SUM(total_volume) AS sector_total_volume,
        AVG(price_change_pct) AS avg_price_change_pct,
        COUNTIF(price_change_pct > 0) AS tickers_up,
        COUNTIF(price_change_pct < 0) AS tickers_down,
        COUNTIF(ABS(price_change_pct) < 0.001) AS tickers_flat
    FROM daily_stats
    GROUP BY sector, date_id
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY date_id ORDER BY avg_price_change_pct DESC) AS sector_performance_rank,
        ROW_NUMBER() OVER (PARTITION BY date_id ORDER BY sector_total_volume DESC) AS sector_volume_rank
    FROM sector_agg
)

SELECT * FROM ranked
