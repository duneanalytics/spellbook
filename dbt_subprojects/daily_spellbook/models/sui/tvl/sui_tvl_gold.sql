{{ config(
    schema='sui_tvl',
    alias='tvl',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'object_type', 'market_id', 'coin_type', 'block_date'],
    partition_by=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','gold']
) }}

-- Final TVL dataset combining DEX pools and lending markets with USD pricing
-- Each row lists the unique object and token pairing with TVL amounts

dex_pools_unpivoted as (
    -- Use DEX gold layer and unpivot to get one row per coin per pool
    select 
        protocol,
        pool_id as market_id,
        coin_type_a as coin_type,
        coin_a_symbol as token_symbol,
        avg_coin_a_amount as token_amount,
        coin_a_price_usd as token_price_usd,
        cast(coalesce(cast(avg_coin_a_amount as double) * coin_a_price_usd, 0) as decimal(38,8)) as token_tvl_usd,
        fee_rate_percent as protocol_fee_rate,
        pool_name,
        'Liquidity Pool' as object_type,
        metric_date as block_date
    from {{ ref('sui_tvl_dex_pools_gold') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('metric_date') }}
    {% endif %}
    
    union all
    
    select 
        protocol,
        pool_id as market_id,
        coin_type_b as coin_type,
        coin_b_symbol as token_symbol,
        avg_coin_b_amount as token_amount,
        coin_b_price_usd as token_price_usd,
        cast(coalesce(cast(avg_coin_b_amount as double) * coin_b_price_usd, 0) as decimal(38,8)) as token_tvl_usd,
        fee_rate_percent as protocol_fee_rate,
        pool_name,
        'Liquidity Pool' as object_type,
        metric_date as block_date
    from {{ ref('sui_tvl_dex_pools_gold') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('metric_date') }}
    {% endif %}
),



all_tvl_data as (
    -- DEX pools data (already has pricing from gold layer)
    select 
        protocol,
        market_id,
        coin_type,
        token_symbol,
        token_amount,
        protocol_fee_rate,
        pool_name as token_name,
        object_type,
        block_date,
        token_price_usd,
        token_tvl_usd as tvl_usd
    from dex_pools_unpivoted
    
    union all
    
    -- Lending pools data (already has pricing and token-level aggregation from gold layer)
    select 
        protocol,
        collateral_coin_symbol as market_id, -- Using token symbol as identifier since aggregated by token
        collateral_coin_type as coin_type,
        collateral_coin_symbol as token_symbol,
        total_collateral_amount as token_amount,
        null as protocol_fee_rate, -- Lending doesn't have explicit fee rates
        concat(protocol, ' - ', coalesce(collateral_coin_symbol, 'UNKNOWN')) as token_name,
        case 
            when protocol = 'bucket' then 'Collateral Vault'
            else 'Lending Pool'
        end as object_type,
        block_date,
        avg_collateral_price_usd as token_price_usd,
        tvl_usd
    from {{ ref('sui_tvl_lending_pools_gold') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
)

select
    protocol,
    object_type,
    market_id,
    coin_type,
    token_symbol,
    token_name, -- e.g. "SUI / USDC 0.30%"
    token_amount as tvl_native_amount,
    tvl_usd,
    protocol_fee_rate,
    token_price_usd,
    block_date
from all_tvl_data
where token_amount > 0 -- Only include pools/markets with actual liquidity
  and tvl_usd > 1000