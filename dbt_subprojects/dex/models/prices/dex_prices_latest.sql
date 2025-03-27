{{ config(
        schema = 'dex',
        alias = 'prices_latest',
        materialized = 'view',
        description = 'DEPRECATED: This model is deprecated and will be removed in the future. Please use prices.latest table instead.'
        )
}}

SELECT
    CAST(date_trunc('month', timestamp) as DATE) as block_month,
    timestamp as block_time,
    contract_address as token_address,
    price as token_price_usd,
    cast(null as double) as token_price_usd_raw  -- Using same value since raw was same as normal in original
FROM {{ source('prices', 'latest') }}
WHERE source = 'dex.trades'