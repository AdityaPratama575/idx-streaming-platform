WITH source AS (
    SELECT DISTINCT sector FROM {{ source('idx_stream', 'top_sector_ticks') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY sector) AS sector_sk,
    sector AS sector_name,
    CASE
        WHEN sector IN ('Financials', 'Properties & Real Estate') THEN 'Financial'
        WHEN sector IN ('Technology', 'Infrastructures', 'Transportation & Logistic') THEN 'Infrastructure & Tech'
        WHEN sector IN ('Energy', 'Basic Materials') THEN 'Commodities'
        ELSE 'Consumer & Services'
    END AS sector_category
FROM source
