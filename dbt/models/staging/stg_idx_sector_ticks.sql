WITH source AS (
    SELECT * FROM {{ source('idx_stream', 'top_sector_ticks') }}
),

renamed AS (
    SELECT
        ticker,
        sector,
        timestamp,
        fetch_ts,
        open,
        high,
        low,
        close,
        volume,
        DATE(timestamp) AS date_id,
        TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket
    FROM source
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
)

SELECT * FROM renamed
