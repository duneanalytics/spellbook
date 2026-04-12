-- ci-stamp: 2
{{
	config(
		schema = 'kalshi',
		alias = 'market_details',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		unique_key = ['ticker'],
		post_hook = '{{ expose_spells(blockchains = \'["kalshi"]\',
								  spell_type = "project",
								  spell_name = "kalshi",
								  contributors = \'["allelosi"]\') }}',
	)
}}

-- Bronze → Gold: Kalshi market reference table
-- Filters to markets with >= 100 contracts traded (99.7% of volume, drops 85% of dust/empty markets)
-- Enriches each market with parent event metadata from market_details_raw
-- Drops 12 columns that are universally null, constant, or internal-only
-- Incremental: watermark_ts = greatest(market snapshot, event snapshot). No incremental_predicates on
-- merge target: event-only updates can leave dest.watermark_ts stale vs the rolling window, which would
-- exclude the row from the merge scan and risk duplicate inserts by ticker.

with markets as (
	select
		m.*
	from {{ source('kalshi', 'markets_raw') }} as m
	where
		m.volume_fp >= 100
	{% if is_incremental() -%}
		and (
			{{ incremental_predicate('m.updated_time') }}
			or m.event_ticker in (
				select ed.event_ticker
				from {{ source('kalshi', 'market_details_raw') }} as ed
				where {{ incremental_predicate('ed.last_updated_ts') }}
			)
		)
	{% endif -%}
)

, event_details as (
	select
		event_ticker,
		series_ticker,
		event_title,
		event_sub_title,
		collateral_return_type,
		mutually_exclusive,
		available_on_brokers,
		product_metadata,
		strike_date,
		strike_period,
		last_updated_ts
	from (
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
			strike_period,
			last_updated_ts,
			row_number() over (partition by event_ticker order by last_updated_ts desc) as rn
		from {{ source('kalshi', 'market_details_raw') }}
	) deduped
	where rn = 1
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

	-- event-level metadata (from market_details_raw)
	ed.series_ticker,
	ed.event_title,
	ed.event_sub_title,
	ed.collateral_return_type,
	ed.mutually_exclusive,
	ed.available_on_brokers,
	ed.product_metadata,
	try(json_extract_scalar(ed.product_metadata, '$.category')) as category,
	try(json_extract_scalar(ed.product_metadata, '$.competition')) as competition,
	ed.strike_date,
	ed.strike_period,

	-- incremental merge watermark (both legs; see model comment)
	greatest(m.updated_time, coalesce(ed.last_updated_ts, m.updated_time)) as watermark_ts

from markets as m
left join event_details as ed
	on m.event_ticker = ed.event_ticker
