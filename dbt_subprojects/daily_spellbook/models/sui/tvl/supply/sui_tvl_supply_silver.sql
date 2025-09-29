{{ config(
    schema='sui_tvl',
    alias='supply_silver',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','supply','silver']
) }}

-- Silver layer: BTC supply with LOCF (Last Observation Carried Forward)

with daily_supply_with_forward_fill as (
    select 
        block_date
        , coin_type
        , coin_symbol
        , coin_decimals
        , total_supply
    from (
        select 
            su.block_date
            , su.coin_type
            , ci.coin_symbol
            , ci.coin_decimals
            -- Get the latest supply for each day, or carry forward the last known value
            , last_value(su.total_supply) ignore nulls over (
                partition by su.coin_type
                order by su.block_date
                rows between unbounded preceding and current row
            ) as total_supply
            , row_number() over (
                partition by su.block_date, su.coin_type 
                order by su.timestamp_ms desc
            ) as rn
        from {{ ref('sui_tvl_supply_bronze') }} su
        inner join {{ ref('sui_tvl_btc_tokens_detail') }} ci
            on su.coin_type = ci.coin_type
        {% if is_incremental() %}
        where {{ incremental_predicate('su.block_date') }}
        {% endif %}
    ) ranked
    where rn = 1  -- Latest update per day per coin
)

-- Deduplicate by symbol (handle multiple tokens with same symbol)
, deduplicated_daily_btc_supply_by_symbol as (
    select
        date
        , coin_symbol
        , carried_forward_supply_raw
        , coin_decimals
    from (
        select
            block_date as date
            , coin_symbol
            , total_supply as carried_forward_supply_raw
            , coin_decimals
            , row_number() over (
                partition by block_date, coin_symbol
                order by coin_type desc -- Tie-break by coin_type
            ) as rn
        from daily_supply_with_forward_fill
        where total_supply is not null
    ) ranked
    where rn = 1
)

select
    date as block_date
    
    -- Total BTC supply across all variants
    , sum(carried_forward_supply_raw / power(10, coin_decimals)) as total_btc_supply
    
    -- Supply breakdown by symbol
    , map_agg(
        coin_symbol
        , round(carried_forward_supply_raw / power(10, coin_decimals), 0)
    ) as supply_breakdown_json
    
from deduplicated_daily_btc_supply_by_symbol
group by date
order by date desc 