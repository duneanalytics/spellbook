{{ config(
    schema='sui_tvl',
    alias='dex_pools_silver',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'protocol', 'pool_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','dex','silver']
) }}

-- Silver layer: Daily aggregated DEX pools with metadata enrichment
-- Handles metadata joins and decimal conversion that bronze doesn't do

with coin_info_cte as (
    select
        coin_type,
        coin_decimals,
        coin_symbol
    from {{ ref('dex_sui_coin_info') }}
),

all_pools_raw as (
    -- Cetus data (standardize column names)
    select
        block_date,
        block_time,
        pool_id,
        coin_type_a,
        coin_type_b,
        coin_a_amount_raw,
        coin_b_amount_raw,
        fee_rate,
        'cetus' as protocol
    from {{ ref('sui_tvl_dex_pools_cetus_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}

    union all

    -- Bluefin data (standardize column names)
    select
        block_date,
        block_time,
        pool_id,
        coin_type_a,
        coin_type_b,
        coin_a_amount_raw,
        coin_b_amount_raw,
        fee_rate,
        'bluefin' as protocol
    from {{ ref('sui_tvl_dex_pools_bluefin_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}

    union all

    -- Momentum data (alias to standard names)
    select
        block_date,
        block_time,
        pool_id,
        token_a_type as coin_type_a,
        token_b_type as coin_type_b,
        reserve_a_raw as coin_a_amount_raw,
        reserve_b_raw as coin_b_amount_raw,
        swap_fee_rate as fee_rate,
        'momentum' as protocol
    from {{ ref('sui_tvl_dex_pools_momentum_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
),

enriched_pools as (
    -- Add metadata and convert amounts
    select
        p.block_date,
        p.block_time,
        p.pool_id,
        p.protocol,
        p.coin_type_a,
        p.coin_type_b,
        
        -- Add coin symbols
        coalesce(coin_a_info.coin_symbol, 'UNKNOWN') as coin_a_symbol,
        coalesce(coin_b_info.coin_symbol, 'UNKNOWN') as coin_b_symbol,
        
        -- Convert raw amounts to decimal
        case when coin_a_info.coin_decimals is not null
            then cast(cast(p.coin_a_amount_raw as double) / 
                 power(10, coin_a_info.coin_decimals) as decimal(38,8))
            else cast(null as decimal(38,8)) end as coin_a_amount,
        case when coin_b_info.coin_decimals is not null
            then cast(cast(p.coin_b_amount_raw as double) / 
                 power(10, coin_b_info.coin_decimals) as decimal(38,8))
            else cast(null as decimal(38,8)) end as coin_b_amount,
        
        -- Convert fee rate to percentage
        p.fee_rate / 10000.0 as fee_rate_percent,
        
        -- Create pool name
        concat(
            coalesce(coin_a_info.coin_symbol, 'UNKNOWN'),
            ' / ',
            coalesce(coin_b_info.coin_symbol, 'UNKNOWN'),
            ' ',
            cast(p.fee_rate / 10000.0 as varchar),
            '%'
        ) as pool_name

    from all_pools_raw p
    left join coin_info_cte coin_a_info on lower(p.coin_type_a) = coin_a_info.coin_type
    left join coin_info_cte coin_b_info on lower(p.coin_type_b) = coin_b_info.coin_type
)

-- Daily aggregation
select
    block_date,
    protocol,
    coin_a_symbol,
    coin_b_symbol,
    coin_type_a,
    coin_type_b,
    pool_id,
    pool_name,
    avg(coin_a_amount) as avg_coin_a_amount,
    avg(coin_b_amount) as avg_coin_b_amount,
    avg(fee_rate_percent) as avg_fee_rate_percent,
    count(*) as num_records,
    max(block_time) as block_time
from enriched_pools
where coin_a_amount > 0 and coin_b_amount > 0  -- Filter out empty pools
group by
    block_date,
    protocol,
    coin_a_symbol,
    coin_b_symbol,
    coin_type_a,
    coin_type_b,
    pool_id,
    pool_name
order by
    block_date desc,
    protocol,
    pool_id 