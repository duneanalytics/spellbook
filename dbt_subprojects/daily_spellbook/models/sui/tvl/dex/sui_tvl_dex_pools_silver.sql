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

-- Silver layer: Daily aggregated DEX pools from all protocols
-- Combines and standardizes data structure across Cetus, Bluefin, and Momentum
-- Performs daily aggregation like the Snowflake pattern

with combined_data as (
    -- Cetus data
    select
        block_date,
        coin_a_symbol,
        coin_b_symbol,
        coin_type_a,
        coin_type_b,
        pool_id,
        pool_name,
        coin_a_amount,
        coin_b_amount,
        fee_rate_percent,
        'cetus' as protocol
    from {{ ref('sui_tvl_dex_pools_cetus_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}

    union all

    -- Bluefin data
    select
        block_date,
        coin_a_symbol,
        coin_b_symbol,
        coin_type_a,
        coin_type_b,
        pool_id,
        pool_name,
        coin_a_amount,
        coin_b_amount,
        fee_rate_percent,
        'bluefin' as protocol
    from {{ ref('sui_tvl_dex_pools_bluefin_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}

    union all

    -- Momentum data (using Momentum-specific column names)
    select
        block_date,
        token_a_symbol as coin_a_symbol, -- Aliased to match standard naming
        token_b_symbol as coin_b_symbol, -- Aliased to match standard naming
        token_a_type as coin_type_a,     -- Aliased to match standard naming
        token_b_type as coin_type_b,     -- Aliased to match standard naming
        pool_id,
        pool_name,
        reserve_a_adjusted as coin_a_amount, -- Aliased to match standard naming
        reserve_b_adjusted as coin_b_amount, -- Aliased to match standard naming
        swap_fee_rate_percent as fee_rate_percent, -- Aliased to match standard naming
        'momentum' as protocol
    from {{ ref('sui_tvl_dex_pools_momentum_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
)

-- Daily aggregation following the Snowflake pattern
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
    max(block_time) as latest_block_time
from combined_data
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