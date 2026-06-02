{{ config(
	schema = 'kalshi',
	alias = 'market_details',
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['ticker'],
	post_hook = '{{ private_data_explorer(blockchains = \'[]\',
	                spell_type = "project",
	                spell_name = "kalshi") }}'
) }}

-- volume_fp >= 100 drops 85% of dust while keeping 99.7% of volume.
-- Merge target is NOT pruned by incremental_predicates: an event-only update on a historical
-- ticker would miss the pruned dest row and INSERT a duplicate, breaking unique(ticker).
--
-- markets_raw.volume_fp can stay frozen because the markets-sync ingest only
-- re-fetches when min_created_ts moves. The ticker WebSocket stream
-- (kalshi.market_updates_raw, added 2026-05-08 via market-feed PR #110) carries
-- live volume_fp per market. We merge the latest live volume in so the >= 100
-- filter operates on the current value, and project the live value downstream.
-- Without this merge, the 2026-05-08 ingest split silently dropped 50-91% of
-- recent Kalshi trades from kalshi.market_trades via the INNER JOIN.

with latest_market_updates as (
	select
		market_ticker
		, max_by(volume_fp, coalesce(ts_ms, ts * 1000)) as volume_fp
	from
		{{ source('kalshi', 'market_updates_raw') }}
	{% if is_incremental() -%}
		where {{ incremental_predicate('from_unixtime(ts)') }}
	{% endif -%}
	group by
		market_ticker
)

, markets as (
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
		-- Project live volume when present; fall back to markets_raw for tickers
		-- with no ticker-stream updates in this incremental window.
		, coalesce(u.volume_fp, m.volume_fp) as volume_fp
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
	left join latest_market_updates as u
		on u.market_ticker = m.ticker
	where
		coalesce(u.volume_fp, m.volume_fp) >= 100
	{% if is_incremental() -%}
		and (
			{{ incremental_predicate('m.updated_time') }}
			-- Tickers whose live volume moved in this incremental window
			-- (latest_market_updates is already scoped via incremental_predicate).
			or u.market_ticker is not null
			or m.event_ticker in (
				select
					ed.event_ticker
				from
					{{ source('kalshi', 'market_details_raw') }} as ed
				where
					{{ incremental_predicate('ed.last_updated_ts') }}
			)
			-- Refreshed series rows: propagate category / frequency / fee_* to existing markets.
			or m.event_ticker in (
				select
					ed.event_ticker
				from
					{{ source('kalshi', 'market_details_raw') }} as ed
				where
					ed.series_ticker in (
						select
							sr.ticker
						from
							{{ source('kalshi', 'series_raw') }} as sr
						where
							{{ incremental_predicate('sr.last_updated_ts') }}
					)
			)
			-- Late-arriving settlements: markets_raw.updated_time can lag ingest by weeks; lifecycle.received_ts is real ingest time.
			-- Filter on field population, not event_type label, so new Kalshi event_types carrying these fields are picked up automatically.
			or m.ticker in (
				select lc.market_ticker
				from {{ source('kalshi', 'markets_lifecycle_raw') }} as lc
				where {{ incremental_predicate('lc.received_ts') }}
					and (lc.result is not null
						or lc.settled_ts is not null
						or lc.determination_ts is not null
						or lc.settlement_value is not null)
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

, series as (
	select
		s.series_ticker
		, s.series_category
		, s.frequency
		, s.fee_type
		, s.fee_multiplier
		, s.series_last_updated_ts
	from (
		select
			ticker as series_ticker
			, category as series_category
			, frequency
			, fee_type
			, fee_multiplier
			, last_updated_ts as series_last_updated_ts
			, row_number() over (partition by ticker order by last_updated_ts desc) as rn
		from
			{{ source('kalshi', 'series_raw') }}
	) as s
	where
		s.rn = 1
)

-- Lifecycle events stream. Filter on column population, not event_type label,
-- so new Kalshi event_types that carry these fields are picked up automatically.
, lifecycle as (
	select
		market_ticker as ticker
		, max_by(result, received_ts) filter (where result is not null) as result
		, max_by(settlement_value, received_ts) filter (where settlement_value is not null) as settlement_value
		, max_by(determination_ts, received_ts) filter (where determination_ts is not null) as determination_ts
		, max_by(settled_ts, received_ts) filter (where settled_ts is not null) as settled_ts
		, max(received_ts) filter (
			where result is not null
				or settled_ts is not null
				or determination_ts is not null
				or settlement_value is not null
		) as last_event_received_ts
	from
		{{ source('kalshi', 'markets_lifecycle_raw') }}
	group by
		market_ticker
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
	-- Lifecycle wins when present: markets_raw settlement fields can lag ingest by weeks.
	, coalesce(lc.settled_ts, m.settlement_ts) as settlement_ts
	, m.status
	, coalesce(lc.result, m.result) as result
	, coalesce(try_cast(lc.settlement_value as double), m.settlement_value_dollars) as settlement_value_dollars
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
	-- Series category is canonical (one row per series); event category is the fallback.
	, coalesce(s.series_category, ed.category) as category
	, try(json_extract_scalar(ed.product_metadata, '$.competition')) as competition
	, ed.strike_date
	, ed.strike_period
	, s.frequency
	, s.fee_type
	, s.fee_multiplier
	-- Includes series_last_updated_ts so downstream trades pick up series-driven dim changes.
	, greatest(
		m.updated_time,
		coalesce(ed.last_updated_ts, m.updated_time),
		coalesce(s.series_last_updated_ts, m.updated_time),
		coalesce(lc.last_event_received_ts, m.updated_time)
	) as source_updated_at
	, now() as _updated_at
from
	markets as m
left join event_details as ed
	on m.event_ticker = ed.event_ticker
left join series as s
	on s.series_ticker = ed.series_ticker
left join lifecycle as lc
	on m.ticker = lc.ticker
