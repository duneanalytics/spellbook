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

with all_dates as (
    select distinct block_date 
    from {{ ref('sui_tvl_supply_bronze') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
),

all_btc_tokens as (
    select coin_type, coin_symbol, coin_decimals
    from {{ ref('sui_tvl_btc_tokens_detail') }}
),

date_token_grid as (
    select 
        d.block_date,
        t.coin_type,
        t.coin_symbol,
        t.coin_decimals
    from all_dates d
    cross join all_btc_tokens t
),

supply_events_with_next as (
    select 
        su.block_date,
        su.coin_type,
        su.total_supply,
        lead(su.block_date) over (
            partition by su.coin_type 
            order by su.block_date
        ) as next_update_date,
        row_number() over (
            partition by su.block_date, su.coin_type 
            order by su.timestamp_ms desc
        ) as rn
    from {{ ref('sui_tvl_supply_bronze') }} su
    inner join {{ ref('sui_tvl_btc_tokens_detail') }} ci
        on su.coin_type = ci.coin_type
    where su.total_supply is not null
),

daily_supply_with_forward_fill as (
    select 
        g.block_date,
        g.coin_type,
        g.coin_symbol,
        g.coin_decimals,
        se.total_supply
    from date_token_grid g
    left join supply_events_with_next se 
        on g.coin_type = se.coin_type
        and g.block_date >= se.block_date
        and (se.next_update_date is null OR g.block_date < se.next_update_date)  -- Standard spellbook forward fill
        and se.rn = 1
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