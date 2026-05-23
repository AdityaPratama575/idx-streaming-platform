WITH stock_data AS (
    SELECT * FROM {{ ref('stg_idx_sector_ticks') }}
),

aggregated AS (
    SELECT
        ticker,
        sector,
        date_id,
        MIN(timestamp) AS first_tick_ts,
        MAX(timestamp) AS last_tick_ts,
        ARRAY_AGG(open ORDER BY timestamp LIMIT 1)[OFFSET(0)] AS open_price,
        ARRAY_AGG(close ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS close_price,
        MAX(high) AS high_price,
        MIN(low) AS low_price,
        SUM(volume) AS total_volume,
        COUNT(*) AS tick_count,
        SAFE_DIVIDE(
            SUM(close * volume), SUM(volume)
        ) AS vwap,
        SAFE_DIVIDE(
            ARRAY_AGG(close ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)]
            - ARRAY_AGG(open ORDER BY timestamp LIMIT 1)[OFFSET(0)],
            ARRAY_AGG(open ORDER BY timestamp LIMIT 1)[OFFSET(0)]
        ) AS price_change_pct
    FROM stock_data
    GROUP BY ticker, sector, date_id
)

SELECT * FROM aggregated
