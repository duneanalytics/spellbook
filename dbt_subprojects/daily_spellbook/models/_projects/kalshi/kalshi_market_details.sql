{{ config(
	schema = 'kalshi',
	alias = 'market_details',
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['ticker'],
) }}

-- Bronze -> Gold: Kalshi market reference table.
-- Filter: markets with >= 100 contracts traded (drops 85% of dust, keeps 99.7% of volume).
-- Incremental source filter is the union of (markets touched) + (markets whose event was touched),
-- so dimension columns from market_details_raw stay fresh even when the market row didn't change.
-- Merge target is NOT pruned by incremental_predicates: an event-only update on a historical
-- ticker would miss the pruned dest row and INSERT a duplicate, breaking unique(ticker).

with markets as (
	select
		m.ticker
		, m.event_ticker
		, m.market_type
		, m.title
		, m.subtitle
		, m.yes_sub_title
		, m.no_sub_title
		, m.created_time
		, m.updated_time
		, m.open_time
		, m.close_time
		, m.expiration_time
		, m.latest_expiration_time
		, m.expected_expiration_time
		, m.settlement_ts
		, m.status
		, m.result
		, m.settlement_value_dollars
		, m.expiration_value
		, m.can_close_early
		, m.early_close_condition
		, m.yes_bid_dollars
		, m.yes_ask_dollars
		, m.no_bid_dollars
		, m.no_ask_dollars
		, m.yes_bid_size_fp
		, m.yes_ask_size_fp
		, m.last_price_dollars
		, m.previous_yes_bid_dollars
		, m.previous_yes_ask_dollars
		, m.previous_price_dollars
		, m.volume_fp
		, m.volume_24h_fp
		, m.open_interest_fp
		, m.strike_type
		, m.floor_strike
		, m.cap_strike
		, m.custom_strike
		, m.tick_size
		, m.fractional_trading_enabled
		, m.rules_primary
		, m.mve_collection_ticker
	from
		{{ source('kalshi', 'markets_raw') }} as m
	where
		m.volume_fp >= 100
	{% if is_incremental() -%}
		and (
			{{ incremental_predicate('m.updated_time') }}
			or m.event_ticker in (
				select
					ed.event_ticker
				from
					{{ source('kalshi', 'market_details_raw') }} as ed
				where
					{{ incremental_predicate('ed.last_updated_ts') }}
			)
		)
	{% endif -%}
)

, event_details as (
	select
		d.event_ticker
		, d.series_ticker
		, d.event_title
		, d.event_sub_title
		, d.category
		, d.collateral_return_type
		, d.mutually_exclusive
		, d.available_on_brokers
		, d.product_metadata
		, d.strike_date
		, d.strike_period
		, d.last_updated_ts
	from (
		select
			ed.event_ticker
			, ed.series_ticker
			, ed.title as event_title
			, ed.sub_title as event_sub_title
			, ed.category
			, ed.collateral_return_type
			, ed.mutually_exclusive
			, ed.available_on_brokers
			, ed.product_metadata
			, ed.strike_date
			, ed.strike_period
			, ed.last_updated_ts
			, row_number() over (partition by ed.event_ticker order by ed.last_updated_ts desc) as rn
		from
			{{ source('kalshi', 'market_details_raw') }} as ed
		inner join (
			select distinct
				m.event_ticker
			from
				markets as m
		) as k
			on ed.event_ticker = k.event_ticker
	) as d
	where
		d.rn = 1
)

select
	m.ticker
	, m.event_ticker
	, m.market_type
	, m.title
	, m.subtitle
	, m.yes_sub_title
	, m.no_sub_title
	, m.created_time
	, m.updated_time
	, m.open_time
	, m.close_time
	, m.expiration_time
	, m.latest_expiration_time
	, m.expected_expiration_time
	, m.settlement_ts
	, m.status
	, m.result
	, m.settlement_value_dollars
	, m.expiration_value
	, m.can_close_early
	, m.early_close_condition
	, m.yes_bid_dollars
	, m.yes_ask_dollars
	, m.no_bid_dollars
	, m.no_ask_dollars
	, m.yes_bid_size_fp
	, m.yes_ask_size_fp
	, m.last_price_dollars
	, m.previous_yes_bid_dollars
	, m.previous_yes_ask_dollars
	, m.previous_price_dollars
	, m.volume_fp
	, m.volume_24h_fp
	, m.open_interest_fp
	, m.strike_type
	, m.floor_strike
	, m.cap_strike
	, m.custom_strike
	, m.tick_size
	, m.fractional_trading_enabled
	, m.rules_primary
	, m.mve_collection_ticker
	, ed.series_ticker
	, ed.event_title
	, ed.event_sub_title
	, ed.collateral_return_type
	, ed.mutually_exclusive
	, ed.available_on_brokers
	, ed.product_metadata
	, ed.category
	, try(json_extract_scalar(ed.product_metadata, '$.competition')) as competition
	, ed.strike_date
	, ed.strike_period
	, greatest(m.updated_time, coalesce(ed.last_updated_ts, m.updated_time)) as source_updated_at
	, now() as _updated_at
from
	markets as m
left join event_details as ed
	on m.event_ticker = ed.event_ticker
