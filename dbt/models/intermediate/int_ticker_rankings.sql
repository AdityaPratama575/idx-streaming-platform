WITH daily_stats AS (
    SELECT * FROM {{ ref('int_daily_stock_stats') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sector, date_id
            ORDER BY total_volume DESC
        ) AS volume_rank
    FROM daily_stats
)

SELECT * FROM ranked
WHERE volume_rank <= 5
