WITH source AS (
    SELECT * FROM {{ source('idx_stream', 'top_sector_ticks') }}
),

ticker_dim AS (
    SELECT * FROM {{ ref('dim_ticker') }}
),

sector_dim AS (
    SELECT * FROM {{ ref('dim_sector') }}
)

SELECT
    d.ticker_sk,
    s.sector_sk,
    src.timestamp,
    src.open,
    src.high,
    src.low,
    src.close,
    src.volume,
    src.fetch_ts
FROM source src
LEFT JOIN ticker_dim d ON src.ticker = d.ticker_code
LEFT JOIN sector_dim s ON src.sector = s.sector_name
