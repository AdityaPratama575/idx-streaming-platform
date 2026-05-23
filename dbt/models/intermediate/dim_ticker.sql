WITH source AS (
    SELECT * FROM {{ source('idx_stream', 'top_sector_ticks') }}
),

ticker_list AS (
    SELECT DISTINCT
        ticker,
        sector,
        MIN(DATE(timestamp)) AS first_seen_date,
        MAX(DATE(timestamp)) AS last_seen_date
    FROM source
    GROUP BY ticker, sector
)

SELECT
    ROW_NUMBER() OVER (ORDER BY ticker) AS ticker_sk,
    ticker AS ticker_code,
    INITCAP(REPLACE(ticker, '.JK', '')) AS ticker_name,
    sector,
    first_seen_date,
    last_seen_date,
    last_seen_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS is_active
FROM ticker_list
