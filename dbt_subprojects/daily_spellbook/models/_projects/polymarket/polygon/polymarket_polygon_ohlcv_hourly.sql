{{ config(
    schema = 'polymarket_polygon',
    alias = 'ohlcv_hourly',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["dpettas"]\') }}'
  )
}}

with base as (
    select
        date_trunc('hour', block_time)          as hour,
        condition_id,
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
    where block_month >= date '2025-01-01'
      and block_time  >= timestamp '2025-01-01'
      and block_time  <  timestamp '2025-04-01'
),

market_meta as (
    select
        token_id,
        condition_id,
        token_outcome,
        tags                                                                    as category,
        market_end_time,
        try_cast(substring(market_end_time from 1 for 19) as timestamp)         as market_end_time_ts,
        outcome                                                                 as market_outcome
    from {{ ref('polymarket_polygon_market_details') }}
),

sparse_ohlcv as (
    select
        b.hour,
        b.condition_id,
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
        count(*)                                                                as trade_count,
        false                                                                   as is_forward_filled
    from base b
    group by b.hour, b.condition_id, b.token_outcome
),

market_bounds as (
    select
        condition_id,
        token_outcome,
        token_id,
        min(hour)                                                               as first_hour,
        least(max(hour), timestamp '2025-03-31 23:00:00')                       as last_hour
    from sparse_ohlcv
    group by condition_id, token_outcome, token_id
),

hour_spine as (
    select
        mb.condition_id,
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
        hs.condition_id,
        hs.token_outcome,
        hs.token_id,
        last_value(s.market_name ignore nulls) over w                           as market_name,
        last_value(s.event_market_name ignore nulls) over w                     as event_market_name,
        last_value(s.open ignore nulls) over w                                  as open,
        last_value(s.high ignore nulls) over w                                  as high,
        last_value(s.low ignore nulls) over w                                   as low,
        last_value(s.close ignore nulls) over w                                 as close,
        case when s.hour is not null then s.vwap end                            as vwap,
        coalesce(s.volume_contracts, 0)                                         as volume_contracts,
        coalesce(s.volume_usd, 0)                                               as volume_usd,
        coalesce(s.trade_count, 0)                                              as trade_count,
        coalesce(s.is_forward_filled, true)                                     as is_forward_filled
    from hour_spine hs
    left join sparse_ohlcv s
        on  hs.condition_id  = s.condition_id
        and hs.token_outcome = s.token_outcome
        and hs.hour          = s.hour
    window w as (
        partition by hs.condition_id, hs.token_outcome
        order by hs.hour
        rows between unbounded preceding and current row
    )
),

with_resolution as (
    select
        f.hour,
        f.condition_id,
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
    r.hour,
    cast(r.condition_id as varchar)                                             as market_id,
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
    r.is_forward_filled
from with_resolution r
