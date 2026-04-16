{{
  config(
    schema = 'kalshi',
    alias = 'market_trades',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["kalshi"]\',
                                  spell_type = "project",
                                  spell_name = "kalshi",
                                  contributors = \'["allelosi"]\') }}'
  )
}}

-- Bronze → Gold: Kalshi trade-level table
-- Only includes trades for markets with >= 100 contracts (via inner join to market_details)
-- Enriches each trade with market title, event, status, and type

with market_details as (
    select
        ticker,
        event_ticker,
        series_ticker,
        market_type,
        title,
        subtitle,
        status,
        result
    from {{ ref('kalshi_market_details') }}
)

select
    t.trade_id,
    t.ticker,
    t.created_time,
    t.taker_side,
    t.count_fp,
    t.yes_price_dollars,
    t.no_price_dollars,
    t.yes_price_dollars * t.count_fp                as amount_usd,
    md.event_ticker,
    md.series_ticker,
    md.market_type,
    md.title,
    md.subtitle,
    md.status,
    md.result,
    now()                                           as _updated_at
from {{ source('kalshi', 'market_trades_0004') }} t
inner join market_details md
    on t.ticker = md.ticker
