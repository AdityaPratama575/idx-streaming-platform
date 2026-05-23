WITH sector_summary AS (
    SELECT * FROM {{ ref('int_sector_daily_summary') }}
)

SELECT
    sector,
    date_id,
    avg_price_change_pct,
    sector_total_volume,
    ticker_count,
    tickers_up,
    tickers_down,
    tickers_flat,
    SAFE_DIVIDE(tickers_up, NULLIF(tickers_down, 0)) AS advance_decline_ratio,
    sector_performance_rank,
    sector_volume_rank
FROM sector_summary
ORDER BY sector_performance_rank
