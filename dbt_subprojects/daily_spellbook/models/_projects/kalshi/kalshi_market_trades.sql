{{ config(
	schema = 'kalshi',
	alias = 'market_trades',
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	partition_by = ['block_month'],
	unique_key = ['block_month', 'trade_id'],
	merge_skip_unchanged = true,
) }}

-- Bronze -> Gold: Kalshi trade-level table.
-- Inner join to market_details (>= 100 contracts). Incremental source filter is the union of
-- (new trades by created_time) + (trades for tickers whose market_details.source_updated_at moved
-- in the window) so dimension columns stay current on historical trades.

with trades_filtered as (
	select
		t.trade_id
		, t.ticker
		, t.created_time
		, t.taker_side
		, t.count_fp
		, t.yes_price_dollars
		, t.no_price_dollars
	from
		{{ source('kalshi', 'market_trades_raw') }} as t
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('t.created_time') }}
		or t.ticker in (
			select
				md.ticker
			from
				{{ ref('kalshi_market_details') }} as md
			where
				{{ incremental_predicate('md.source_updated_at') }}
		)
	{% endif -%}
)

, market_details as (
	select
		md.ticker
		, md.event_ticker
		, md.series_ticker
		, md.market_type
		, md.title
		, md.subtitle
		, md.status
		, md.result
	from
		{{ ref('kalshi_market_details') }} as md
	inner join (
		select distinct
			tr.ticker
		from
			trades_filtered as tr
	) as k
		on md.ticker = k.ticker
)

select
	t.trade_id
	, t.ticker
	, t.created_time
	, cast(date_trunc('month', t.created_time) as date) as block_month
	, t.taker_side
	, t.count_fp
	, t.yes_price_dollars
	, t.no_price_dollars
	, t.yes_price_dollars * t.count_fp as amount_usd
	, md.event_ticker
	, md.series_ticker
	, md.market_type
	, md.title
	, md.subtitle
	, md.status
	, md.result
	, now() as _updated_at
from
	trades_filtered as t
inner join market_details as md
	on t.ticker = md.ticker
