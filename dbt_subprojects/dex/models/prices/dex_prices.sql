{{ config(
    schema = 'dex',
    alias = 'prices',
    materialized = 'view',
    description = 'DEPRECATED: This model is deprecated and will be removed in the future. Please use prices.hour table instead'
    )
}}

SELECT 
    CAST(date_trunc('month', timestamp) as date) as block_month, -- for partitioning 
    timestamp as hour,
    contract_address,
    blockchain,
    price as median_price,
    cast(null as int) as sample_size
FROM {{ source('prices_dex', 'hour') }}
