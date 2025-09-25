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

-- Gold layer: BTC lending pools with protocol-specific adjustments
-- Following the Snowflake BTC_LENDING_POOLS_DAILY_GOLD pattern exactly
-- Filters for BTC tokens only and applies protocol-specific decimal corrections

select
    block_date as date,
    protocol,
    collateral_coin_symbol,
    
    -- BTC Collateral (with protocol-specific adjustments)
    sum(
        case 
            when protocol = 'navi' and collateral_coin_symbol like '%BTC%' 
            then eod_collateral_amount / 10
            else eod_collateral_amount 
        end
    ) as btc_collateral,
    
    -- BTC Borrow (with protocol-specific adjustments)
    sum(
        case 
            when protocol = 'bucket' then 0  -- Bucket borrows BUCK, not BTC
            when protocol = 'navi' and collateral_coin_symbol like '%BTC%' 
            then eod_borrow_amount / 10
            when protocol = 'suilend' and collateral_coin_symbol like '%BTC%'
            then eod_borrow_amount / power(10, 18)
            else eod_borrow_amount 
        end
    ) as btc_borrow,
    
    -- BTC Supply (collateral - borrow with adjustments)
    sum(
        case 
            when protocol = 'bucket' then 
                case 
                    when protocol = 'navi' and collateral_coin_symbol like '%BTC%' 
                    then eod_collateral_amount / 10
                    else eod_collateral_amount 
                end  -- For Bucket, supply = collateral (no BTC borrow)
            when protocol = 'navi' and collateral_coin_symbol like '%BTC%' 
            then (eod_collateral_amount - eod_borrow_amount) / 10
            when protocol = 'suilend' and collateral_coin_symbol like '%BTC%'
            then (eod_collateral_amount - eod_borrow_amount / power(10, 18))
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