{{ config(
    schema = 'polymarket_polygon',
    alias = 'ohlcv_hourly',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["0xboxer"]\') }}'
  )
}}

with base as (
    select
        date_trunc('hour', block_time)          as hour,
        condition_id,
        token_outcome,
        question                                as market_name,
        event_market_name,
        price,
        amount                                  as usd_notional,
        shares                                  as contracts,
        price * shares                          as price_x_shares,
        row_number() over (
            partition by condition_id, token_outcome, date_trunc('hour', block_time)
            order by block_time asc
        )                                       as rn_first,
        row_number() over (
            partition by condition_id, token_outcome, date_trunc('hour', block_time)
            order by block_time desc
        )                                       as rn_last
    from {{ ref('polymarket_polygon_market_trades') }}
    where block_month >= date '2025-01-01'
      and block_time  >= timestamp '2025-01-01'
      and block_time  <  timestamp '2026-01-01'
),

market_meta as (
    select
        condition_id,
        tags                as category,
        market_end_time,
        outcome             as market_outcome,
        event_market_name
    from {{ ref('polymarket_polygon_market_details') }}
)

select
    b.hour,
    cast(b.condition_id as varchar)                                     as market_id,
    max(b.market_name)                                                  as market_name,
    b.token_outcome                                                     as outcome,
    max(m.category)                                                     as category,
    round(max(case when b.rn_first = 1 then b.price end), 4)           as open,
    round(max(b.price), 4)                                              as high,
    round(min(b.price), 4)                                              as low,
    round(max(case when b.rn_last  = 1 then b.price end), 4)           as close,
    round(sum(b.price_x_shares) / nullif(sum(b.contracts), 0), 4)      as vwap,
    round(sum(b.contracts), 2)                                          as volume_contracts,
    round(sum(b.usd_notional), 2)                                      as volume_usd,
    count(*)                                                            as trade_count,
    max(m.market_end_time)                                              as market_end_time,
    max(m.market_outcome)                                               as market_outcome,
    max(b.event_market_name)                                            as event_market_name
from base b
left join market_meta m
    on cast(b.condition_id as varchar) = m.condition_id
group by b.hour, b.condition_id, b.token_outcome
