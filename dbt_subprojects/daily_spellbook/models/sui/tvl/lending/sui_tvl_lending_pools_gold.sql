{{ config(
    schema='sui_tvl',
    alias='lending_pools_gold',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'protocol', 'collateral_coin_symbol'],
    partition_by=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','lending','gold']
) }}

-- Gold layer: BTC lending pools with standard decimal normalization
-- Removed arbitrary protocol-specific adjustments, using proper decimal handling
-- Filters for BTC tokens only and aggregates by protocol and token symbol

select
    block_date as date,
    protocol,
    collateral_coin_symbol,
    
    -- BTC Collateral (standard decimal normalization)
    sum(eod_collateral_amount) as btc_collateral,
    
    -- BTC Borrow (standard decimal normalization, Bucket borrows BUCK not BTC)
    sum(
        case 
            when protocol = 'bucket' then 0  -- Bucket borrows BUCK, not BTC
            else eod_borrow_amount 
        end
    ) as btc_borrow,
    
    -- BTC Supply (collateral - borrow with standard calculation)
    sum(
        case 
            when protocol = 'bucket' then eod_collateral_amount  -- For Bucket, supply = collateral (no BTC borrow)
            else (eod_collateral_amount - eod_borrow_amount)
        end
    ) as btc_supply
    
from {{ ref('sui_tvl_lending_pools_silver') }}
where collateral_coin_symbol like '%BTC%'  -- BTC filtering like Snowflake
{% if is_incremental() %}
and {{ incremental_predicate('block_date') }}
{% endif %}
group by
    block_date,
    protocol,
    collateral_coin_symbol
order by
    block_date desc,
    protocol,
    collateral_coin_symbol 