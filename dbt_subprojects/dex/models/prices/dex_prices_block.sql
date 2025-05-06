{{ config(
    schema = 'dex'
    , alias = 'prices_block'
    , materialized = 'view'
    , description = 'DEPRECATED: This model is deprecated and only exists to maintain backward compatibility with existing queries. Please use prices.day, prices.hour, or prices.minute for new development.'
) }}

select
    blockchain
    , contract_address
    , symbol
    , decimals
    , CAST(NULL as bigint) as block_number  -- block_number not available in minute_updates
    , cast(date_trunc('month', timestamp) as date) as block_month
    , timestamp as block_time
    , CASE 
        WHEN price > 0 THEN volume / price  -- derive amount from volume and price
        ELSE 0 
      END as amount
    , volume as amount_usd
    , price
from 
    {{ source('prices', 'minute_updates') }}
where
    source = 'dex.trades'  -- Only include DEX-sourced prices for consistency