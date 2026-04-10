{{ config(
    schema = 'kalshi',
    alias = 'ohlcv_hourly',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    post_hook = '{{ expose_spells(blockchains = \'["kalshi"]\',
                                  spell_type = "project",
                                  spell_name = "kalshi",
                                  contributors = \'["allelosi"]\') }}'
  )
}}

-- Hourly OHLCV candles for Kalshi prediction markets
-- Built on yes_price_dollars from kalshi_market_trades
-- Forward-fills no-trade hours, resolution-corrects close prices

with base as (
    select
        date_trunc('hour', created_time)            as hour,
        ticker,
        title                                       as market_name,
        event_ticker,
        yes_price_dollars                           as price,
        count_fp                                    as contracts,
        yes_price_dollars * count_fp                as usd_notional,
        yes_price_dollars * count_fp                as price_x_shares,
        row_number() over (
            partition by ticker, date_trunc('hour', created_time)
            order by created_time asc, trade_id asc
        )                                           as rn_first,
        row_number() over (
            partition by ticker, date_trunc('hour', created_time)
            order by created_time desc, trade_id desc
        )                                           as rn_last
    from {{ ref('kalshi_market_trades') }}
),

market_meta as (
    select
        ticker,
        event_ticker,
        title,
        status,
        result,
        expiration_time,
        product_metadata,
        try(json_extract_scalar(product_metadata, '$.category')) as category
    from {{ ref('kalshi_market_details') }}
),

sparse_ohlcv as (
    select
        b.hour,
        b.ticker,
        max(b.market_name)                                                      as market_name,
        max(b.event_ticker)                                                     as event_ticker,
        round(max(case when b.rn_first = 1 then b.price end), 6)              as open,
        round(max(b.price), 6)                                                  as high,
        round(min(b.price), 6)                                                  as low,
        round(max(case when b.rn_last  = 1 then b.price end), 6)              as close,
        round(sum(b.price_x_shares) / nullif(sum(b.contracts), 0), 6)         as vwap,
        round(sum(b.contracts), 6)                                              as volume_contracts,
        round(sum(b.usd_notional), 6)                                           as volume_usd,
        count(*)                                                                as trade_count,
        false                                                                   as is_forward_filled
    from base b
    group by b.hour, b.ticker
),

market_bounds as (
    select
        ticker,
        min(hour)                                                               as first_hour,
        max(hour)                                                               as last_hour
    from sparse_ohlcv
    group by ticker
),

hour_spine as (
    select
        mb.ticker,
        h.timestamp                                                             as hour
    from market_bounds mb
    cross join {{ source('utils', 'hours') }} h
    where h.timestamp >= mb.first_hour
      and h.timestamp <= mb.last_hour
),

filled as (
    select
        hs.hour,
        hs.ticker,
        s.market_name,
        s.event_ticker,
        s.open,
        s.high,
        s.low,
        s.close,
        case when hs.hour = s.hour then s.vwap end                             as vwap,
        case when hs.hour = s.hour then s.volume_contracts else 0 end           as volume_contracts,
        case when hs.hour = s.hour then s.volume_usd else 0 end                as volume_usd,
        case when hs.hour = s.hour then s.trade_count else 0 end               as trade_count,
        hs.hour != s.hour                                                       as is_forward_filled
    from hour_spine hs
    asof left join sparse_ohlcv s
        on  s.ticker = hs.ticker
        and s.hour  <= hs.hour
),

with_resolution as (
    select
        f.hour,
        f.ticker,
        f.market_name,
        f.event_ticker,
        f.open,
        f.high,
        f.low,
        case
            when m.expiration_time is not null
                 and f.hour > m.expiration_time
                 and m.result in ('yes', 'no')
            then
                case
                    when m.result = 'yes' then 1.0
                    when m.result = 'no'  then 0.0
                    else f.close
                end
            else f.close
        end                                                                     as close,
        f.vwap,
        f.volume_contracts,
        f.volume_usd,
        f.trade_count,
        m.category,
        m.expiration_time                                                       as market_end_time,
        m.result                                                                as market_outcome,
        m.title                                                                 as event_name,
        f.is_forward_filled
    from filled f
    left join market_meta m
        on f.ticker = m.ticker
)

select
    r.hour,
    r.ticker                                                                    as market_id,
    r.market_name,
    'Yes'                                                                       as outcome,
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
    r.event_name,
    r.is_forward_filled
from with_resolution r
