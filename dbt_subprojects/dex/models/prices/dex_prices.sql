{{ config(
    schema = 'dex',
    alias = 'prices',
    materialized = 'view',
    description = 'DEPRECATED: This model is deprecated and will be removed in the future. Please use prices.hour table instead'
    )
}}

SELECT 
    CAST(date_trunc('month', hour) as date) as block_month, -- for partitioning 
    hour,
    contract_address,
    blockchain,
    median_price,
    sample_size
FROM (
    SELECT 
        date_trunc('hour', timestamp) as hour,
        contract_address,
        blockchain,
        approx_percentile(price, 0.5) AS median_price,
        COUNT(*) as sample_size -- Count of minute-level observations per hour
    FROM {{ source('prices', 'minute_updates') }}
    WHERE source = 'dex.trades'
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 5 -- Ensure we have at least 5 minute-level observations per hour
) tmp