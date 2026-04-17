{{ config(
	schema = 'kalshi',
	alias = 'ohlcv_hourly',
	materialized = 'table',
	file_format = 'delta',
) }}

-- Hourly OHLCV candles for Kalshi prediction markets (Yes price only; No is implied as 1 - Yes)
-- Full history: all trades from the underlying market_trades table

with base as (
	select
		date_trunc('hour', t.created_time) as hour,
		t.ticker,
		t.title as market_name,
		t.event_ticker,
		t.yes_price_dollars as price,
		t.count_fp as contracts,
		t.amount_usd as usd_notional,
		t.yes_price_dollars * t.count_fp as price_x_shares,
		row_number() over (
			partition by t.ticker, date_trunc('hour', t.created_time)
			order by t.created_time asc, t.trade_id asc
		) as rn_first,
		row_number() over (
			partition by t.ticker, date_trunc('hour', t.created_time)
			order by t.created_time desc, t.trade_id desc
		) as rn_last
	from {{ ref('kalshi_market_trades') }} as t
)

, market_meta as (
	select
		md.ticker,
		md.event_ticker,
		md.title,
		md.event_title,
		md.status,
		md.result,
		md.expiration_time,
		md.category
	from {{ ref('kalshi_market_details') }} as md
)

, sparse_ohlcv as (
	select
		b.hour,
		b.ticker,
		max(b.market_name) as market_name,
		max(b.event_ticker) as event_ticker,
		round(max(case when b.rn_first = 1 then b.price end), 6) as open,
		round(max(b.price), 6) as high,
		round(min(b.price), 6) as low,
		round(max(case when b.rn_last = 1 then b.price end), 6) as close,
		round(sum(b.price_x_shares) / nullif(sum(b.contracts), 0), 6) as vwap,
		round(sum(b.contracts), 6) as volume_contracts,
		round(sum(b.usd_notional), 6) as volume_usd,
		count(*) as trade_count,
		false as is_forward_filled
	from base as b
	group by
		b.hour,
		b.ticker
)

, market_bounds as (
	select
		s.ticker,
		min(s.hour) as first_hour,
		least(max(s.hour), date_trunc('hour', now())) as last_hour
	from sparse_ohlcv as s
	group by
		s.ticker
)

, hour_spine as (
	select
		mb.ticker,
		h.timestamp as hour
	from market_bounds as mb
	cross join {{ source('utils', 'hours') }} as h
	where
		h.timestamp >= mb.first_hour
		and h.timestamp <= mb.last_hour
)

, filled as (
	select
		hs.hour,
		hs.ticker,
		s.market_name,
		s.event_ticker,
		s.open,
		s.high,
		s.low,
		s.close,
		case when hs.hour = s.hour then s.vwap end as vwap,
		case when hs.hour = s.hour then s.volume_contracts else 0 end as volume_contracts,
		case when hs.hour = s.hour then s.volume_usd else 0 end as volume_usd,
		case when hs.hour = s.hour then s.trade_count else 0 end as trade_count,
		hs.hour != s.hour as is_forward_filled
	from hour_spine as hs
	asof left join sparse_ohlcv as s
		on s.ticker = hs.ticker
		and s.hour <= hs.hour
)

, with_resolution as (
	select
		f.hour,
		f.ticker,
		f.market_name,
		f.event_ticker,
		case
			when m.expiration_time is not null
				and f.hour > m.expiration_time
				and m.result in ('yes', 'no')
			then case when m.result = 'yes' then 1.0 else 0.0 end
			else f.open
		end as open,
		case
			when m.expiration_time is not null
				and f.hour > m.expiration_time
				and m.result in ('yes', 'no')
			then case when m.result = 'yes' then 1.0 else 0.0 end
			else f.high
		end as high,
		case
			when m.expiration_time is not null
				and f.hour > m.expiration_time
				and m.result in ('yes', 'no')
			then case when m.result = 'yes' then 1.0 else 0.0 end
			else f.low
		end as low,
		case
			when m.expiration_time is not null
				and f.hour > m.expiration_time
				and m.result in ('yes', 'no')
			then case when m.result = 'yes' then 1.0 else 0.0 end
			else f.close
		end as close,
		f.vwap,
		f.volume_contracts,
		f.volume_usd,
		f.trade_count,
		m.category,
		m.expiration_time as market_end_time,
		m.result as market_outcome,
		coalesce(m.event_title, m.title) as event_market_name,
		f.is_forward_filled
	from filled as f
	left join market_meta as m
		on f.ticker = m.ticker
)

select
	r.hour,
	r.ticker as market_id,
	r.market_name,
	'Yes' as outcome,
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
from with_resolution as r
