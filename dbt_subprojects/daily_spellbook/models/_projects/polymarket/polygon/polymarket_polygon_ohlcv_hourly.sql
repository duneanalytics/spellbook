{{ config(
    schema = 'polymarket_polygon',
    alias = 'ohlcv_hourly',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'hour', 'market_id', 'outcome'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.hour')],
    merge_skip_unchanged = true,
  )
}}

with base as (
    select
        date_trunc('hour', block_time)          as hour,
        cast(condition_id as varchar)           as market_id,
        token_outcome,
        asset_id,
        question                                as market_name,
        event_market_name,
        price,
        amount                                  as usd_notional,
        shares                                  as contracts,
        price * shares                          as price_x_shares,
        row_number() over (
            partition by condition_id, token_outcome, date_trunc('hour', block_time)
            order by block_time asc, evt_index asc nulls last, tx_hash asc nulls last
        )                                       as rn_first,
        row_number() over (
            partition by condition_id, token_outcome, date_trunc('hour', block_time)
            order by block_time desc, evt_index desc nulls last, tx_hash desc nulls last
        )                                       as rn_last
    from {{ ref('polymarket_polygon_market_trades') }}
    where token_outcome is not null
    {% if is_incremental() -%}
      and {{ incremental_predicate('block_time') }}
    {%- endif %}
),

market_meta as (
    select
        token_id,
        cast(condition_id as varchar)                                           as market_id,
        token_outcome,
        tags                                                                    as category,
        market_end_time,
        try_cast(substring(market_end_time from 1 for 19) as timestamp)         as market_end_time_ts,
        outcome                                                                 as market_outcome
    from {{ ref('polymarket_polygon_market_details') }}
),

new_sparse as (
    select
        b.hour,
        b.market_id,
        b.token_outcome,
        max(b.asset_id)                                                         as token_id,
        max(b.market_name)                                                      as market_name,
        max(b.event_market_name)                                                as event_market_name,
        round(max(case when b.rn_first = 1 then b.price end), 6)               as open,
        round(max(b.price), 6)                                                  as high,
        round(min(b.price), 6)                                                  as low,
        round(max(case when b.rn_last  = 1 then b.price end), 6)               as close,
        round(sum(b.price_x_shares) / nullif(sum(b.contracts), 0), 6)          as vwap,
        round(sum(b.contracts), 6)                                              as volume_contracts,
        round(sum(b.usd_notional), 6)                                           as volume_usd,
        count(*)                                                                as trade_count
    from base b
    group by b.hour, b.market_id, b.token_outcome
),

{% if is_incremental() -%}
-- pre-window sparse anchor from {{ this }} so market_bounds and asof forward-fill stay correct across the window boundary
prior_sparse as (
    select
        t.hour,
        t.market_id,
        t.outcome                                                               as token_outcome,
        cast(null as uint256)                                                   as token_id,
        t.market_name,
        t.event_market_name,
        t.open,
        t.high,
        t.low,
        t.close,
        t.vwap,
        t.volume_contracts,
        t.volume_usd,
        t.trade_count
    from {{ this }} t
    where t.is_forward_filled = false
      and not {{ incremental_predicate('t.hour') }}
),
{% endif %}

sparse_ohlcv as (
    select
        hour, market_id, token_outcome, token_id, market_name, event_market_name,
        open, high, low, close, vwap, volume_contracts, volume_usd, trade_count
    from new_sparse
    {% if is_incremental() -%}
    union all
    select
        hour, market_id, token_outcome, token_id, market_name, event_market_name,
        open, high, low, close, vwap, volume_contracts, volume_usd, trade_count
    from prior_sparse
    {%- endif %}
),

market_bounds as (
    select
        market_id,
        token_outcome,
        max(token_id)                                                           as token_id,
        min(hour)                                                               as first_hour,
        max(hour)                                                               as last_hour
    from sparse_ohlcv
    group by market_id, token_outcome
),

hour_spine as (
    select
        mb.market_id,
        mb.token_outcome,
        mb.token_id,
        h.timestamp                                                             as hour
    from market_bounds mb
    cross join {{ source('utils', 'hours') }} h
    where h.timestamp >= mb.first_hour
      and h.timestamp <= mb.last_hour
),

filled as (
    select
        hs.hour,
        hs.market_id,
        hs.token_outcome,
        hs.token_id,
        s.market_name,
        s.event_market_name,
        s.open,
        s.high,
        s.low,
        s.close,
        case when hs.hour = s.hour then s.vwap end                             as vwap,
        case when hs.hour = s.hour then s.volume_contracts else 0 end           as volume_contracts,
        case when hs.hour = s.hour then s.volume_usd else 0 end                as volume_usd,
        case when hs.hour = s.hour then s.trade_count else 0 end                as trade_count,
        hs.hour != s.hour                                                       as is_forward_filled
    from hour_spine hs
    asof left join sparse_ohlcv s
        on  s.market_id     = hs.market_id
        and s.token_outcome = hs.token_outcome
        and s.hour         <= hs.hour
),

with_resolution as (
    select
        f.hour,
        f.market_id,
        f.token_outcome,
        f.token_id,
        f.market_name,
        f.event_market_name,
        f.open,
        f.high,
        f.low,
        case
            when m.market_end_time_ts is not null
                 and f.hour > m.market_end_time_ts
                 and m.market_outcome is not null
            then
                case
                    when f.token_outcome = 'Yes' and m.market_outcome = 'yes' then 1.0
                    when f.token_outcome = 'Yes' and m.market_outcome = 'no'  then 0.0
                    when f.token_outcome = 'No'  and m.market_outcome = 'yes' then 0.0
                    when f.token_outcome = 'No'  and m.market_outcome = 'no'  then 1.0
                    else f.close
                end
            else f.close
        end                                                                     as close,
        f.vwap,
        f.volume_contracts,
        f.volume_usd,
        f.trade_count,
        m.category,
        m.market_end_time,
        m.market_outcome,
        f.is_forward_filled
    from filled f
    left join market_meta m
        on f.token_id = m.token_id
)

select
    cast(date_trunc('month', r.hour) as date)                                   as block_month,
    r.hour,
    r.market_id,
    r.market_name,
    r.token_outcome                                                             as outcome,
    r.category,
    r.open,
    r.high,
    r.low,
    r.close,
    r.vwap,
    r.volume_contracts,
    r.volume_usd,
    r.trade_count,
    r.market_end_time,
    r.market_outcome,
    r.event_market_name,
    r.is_forward_filled,
    now()                                                                       as _updated_at
from with_resolution r
{% if is_incremental() -%}
where {{ incremental_predicate('r.hour') }}
{%- endif %}
