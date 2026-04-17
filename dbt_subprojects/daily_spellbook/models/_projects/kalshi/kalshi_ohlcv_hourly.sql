{{ config(
	schema = 'kalshi',
	alias = 'ohlcv_hourly',
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	partition_by = ['block_month'],
	unique_key = ['block_month', 'hour', 'market_id', 'outcome'],
	incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.hour')],
	merge_skip_unchanged = true,
) }}

with base as (
	select
		date_trunc('hour', t.created_time) as hour,
		t.ticker as market_id,
		t.title as market_name,
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
	{% if is_incremental() -%}
	where {{ incremental_predicate('t.created_time') }}
	{%- endif %}
),

market_meta as (
	select
		md.ticker as market_id,
		md.title,
		md.event_title,
		md.result,
		md.expiration_time,
		md.category
	from {{ ref('kalshi_market_details') }} as md
),

new_sparse as (
	select
		b.hour,
		b.market_id,
		cast('Yes' as varchar) as outcome,
		max(b.market_name) as market_name,
		round(max(case when b.rn_first = 1 then b.price end), 6) as open,
		round(max(b.price), 6) as high,
		round(min(b.price), 6) as low,
		round(max(case when b.rn_last = 1 then b.price end), 6) as close,
		round(sum(b.price_x_shares) / nullif(sum(b.contracts), 0), 6) as vwap,
		round(sum(b.contracts), 6) as volume_contracts,
		round(sum(b.usd_notional), 6) as volume_usd,
		count(*) as trade_count
	from base as b
	group by b.hour, b.market_id
),

{% if is_incremental() -%}
-- pre-window sparse anchor from {{ this }} so market_bounds and asof forward-fill stay correct across the window boundary
prior_sparse as (
	select
		t.hour,
		t.market_id,
		t.outcome,
		t.market_name,
		t.open,
		t.high,
		t.low,
		t.close,
		t.vwap,
		t.volume_contracts,
		t.volume_usd,
		t.trade_count
	from {{ this }} as t
	where t.is_forward_filled = false
		and not {{ incremental_predicate('t.hour') }}
),
{% endif %}

sparse_ohlcv as (
	select
		hour, market_id, outcome, market_name,
		open, high, low, close, vwap, volume_contracts, volume_usd, trade_count
	from new_sparse
	{% if is_incremental() -%}
	union all
	select
		hour, market_id, outcome, market_name,
		open, high, low, close, vwap, volume_contracts, volume_usd, trade_count
	from prior_sparse
	{%- endif %}
),

market_bounds as (
	select
		s.market_id,
		s.outcome,
		min(s.hour) as first_hour,
		least(max(s.hour), date_trunc('hour', now())) as last_hour
	from sparse_ohlcv as s
	group by s.market_id, s.outcome
),

hour_spine as (
	select
		mb.market_id,
		mb.outcome,
		h.timestamp as hour
	from market_bounds as mb
	cross join {{ source('utils', 'hours') }} as h
	where h.timestamp >= mb.first_hour
		and h.timestamp <= mb.last_hour
),

filled as (
	select
		hs.hour,
		hs.market_id,
		hs.outcome,
		s.market_name,
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
		on s.market_id = hs.market_id
		and s.outcome = hs.outcome
		and s.hour <= hs.hour
),

with_resolution as (
	select
		f.hour,
		f.market_id,
		f.outcome,
		f.market_name,
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
		on f.market_id = m.market_id
)

select
	cast(date_trunc('month', r.hour) as date) as block_month,
	r.hour,
	r.market_id,
	r.market_name,
	r.outcome,
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
	r.is_forward_filled,
	now() as _updated_at
from with_resolution as r
{% if is_incremental() -%}
where {{ incremental_predicate('r.hour') }}
{%- endif %}
