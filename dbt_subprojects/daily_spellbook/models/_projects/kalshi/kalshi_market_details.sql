{{
  config(
    schema = 'kalshi',
    alias = 'market_details',
    materialized = 'table',
    post_hook = '{{ expose_spells(blockchains = \'["kalshi"]\',
                                  spell_type = "project",
                                  spell_name = "kalshi",
                                  contributors = \'["dpettas"]\') }}'
  )
}}

-- Bronze → Gold: Kalshi market reference table
-- Filters to markets with >= 100 contracts traded (99.7% of volume, drops 85% of dust/empty markets)
-- Enriches each market with parent event metadata from market_details_0003
-- Drops 12 columns that are universally null, constant, or internal-only

with markets as (
    select *
    from {{ source('kalshi', 'markets_0003') }}
    where volume_fp >= 100
)

, event_details as (
    select
        event_ticker,
        series_ticker,
        title as event_title,
        sub_title as event_sub_title,
        collateral_return_type,
        mutually_exclusive,
        available_on_brokers,
        product_metadata,
        strike_date,
        strike_period
    from {{ source('kalshi', 'market_details_0003') }}
)

select
    -- identifiers
    m.ticker,
    m.event_ticker,
    m.market_type,

    -- naming
    m.title,
    m.subtitle,
    m.yes_sub_title,
    m.no_sub_title,

    -- timestamps
    m.created_time,
    m.updated_time,
    m.open_time,
    m.close_time,
    m.expiration_time,
    m.latest_expiration_time,
    m.expected_expiration_time,
    m.settlement_ts,

    -- status & settlement
    m.status,
    m.result,
    m.settlement_value_dollars,
    m.expiration_value,
    m.can_close_early,
    m.early_close_condition,

    -- pricing snapshot (latest state)
    m.yes_bid_dollars,
    m.yes_ask_dollars,
    m.no_bid_dollars,
    m.no_ask_dollars,
    m.yes_bid_size_fp,
    m.yes_ask_size_fp,
    m.last_price_dollars,
    m.previous_yes_bid_dollars,
    m.previous_yes_ask_dollars,
    m.previous_price_dollars,

    -- volume & open interest
    m.volume_fp,
    m.volume_24h_fp,
    m.open_interest_fp,

    -- strike structure
    m.strike_type,
    m.floor_strike,
    m.cap_strike,
    m.custom_strike,
    m.tick_size,
    m.fractional_trading_enabled,

    -- rules
    m.rules_primary,

    -- event-level metadata (from market_details_0003)
    ed.series_ticker,
    ed.event_title,
    ed.event_sub_title,
    ed.collateral_return_type,
    ed.mutually_exclusive,
    ed.available_on_brokers,
    ed.product_metadata,
    ed.strike_date,
    ed.strike_period

from markets m
left join event_details ed
    on m.event_ticker = ed.event_ticker
