{{ config(
	schema='lido_liquidity_unichain',
	alias='uniswap_v4_pools',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['pool', 'time'],
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.time')],
) }}

{% set project_start_date = '2025-04-01' %}

with pools as (
	select
		*
	from
		{{ ref('lido_liquidity_unichain_stg_uniswap_v4_pools') }}
)
, tokens as (
	select distinct
		token as address
	from
		(
			select token0 as token
			from pools
			union all
			select token1
			from pools
			union all
			select 0x4200000000000000000000000000000000000006
		) as t
)
, tokens_prices_daily as (
	select
		p.timestamp as time
		, if(p.contract_address = 0x4200000000000000000000000000000000000006, 0x0000000000000000000000000000000000000000, p.contract_address) as token
		, p.decimals
		, if(p.symbol = 'WETH', 'ETH', p.symbol) as symbol
		, p.price
	from
		{{ source('prices','day') }} as p
	inner join tokens as t
		on p.contract_address = t.address
	where
		p.blockchain = 'unichain'
		{% if not is_incremental() -%}
		and p.timestamp >= date '{{ project_start_date }}'
		{% else -%}
		and {{ incremental_predicate('p.timestamp') }}
		{% endif -%}
)
, tokens_prices_hourly as (
	select
		p.timestamp as time
		, p.timestamp + interval '1' hour as next_time
		, if(p.contract_address = 0x4200000000000000000000000000000000000006, 0x0000000000000000000000000000000000000000, p.contract_address) as token
		, p.decimals
		, if(p.symbol = 'WETH', 'ETH', p.symbol) as symbol
		, p.price
	from
		{{ source('prices','hour') }} as p
	inner join tokens as t
		on p.contract_address = t.address
	where
		p.blockchain = 'unichain'
		{% if not is_incremental() -%}
		and p.timestamp >= timestamp '{{ project_start_date }}'
		{% else -%}
		and {{ incremental_predicate('p.timestamp') }}
		{% endif -%}
)
, swap_events_hourly as (
	select
		sw.evt_block_time as time
		, sw.pool_id as pool
		, sw.token0
		, sw.token1
		, coalesce(sum(cast(abs(sw.amount0) as double)), 0) as amount0
		, coalesce(sum(cast(abs(sw.amount1) as double)), 0) as amount1
	from
		{{ ref('lido_liquidity_unichain_stg_uniswap_v4_swaps') }} as sw
	{% if not is_incremental() -%}
	where sw.evt_block_date >= date '{{ project_start_date }}'
	{% else -%}
	where {{ incremental_predicate('sw.evt_block_date') }}
	{% endif -%}
	group by
		1, 2, 3, 4
)
, trading_volume as (
	select
		date_trunc('day', s.time) as time
		, s.pool
		, sum(case when p0.decimals is not null
			then (p0.price * s.amount0) / cast(power(10, p0.decimals) as double)
			else (p1.price * s.amount1) / cast(power(10, p1.decimals) as double)
			end) as volume
	from
		swap_events_hourly as s
	left join tokens_prices_hourly as p0
        on date_trunc('hour', s.time) >= p0.time
        and date_trunc('hour', s.time) <  p0.next_time
		and p0.token = s.token0
	left join tokens_prices_hourly as p1
        on date_trunc('hour', s.time) >= p1.time
        and date_trunc('hour', s.time) <  p1.next_time
		and p1.token = s.token1
	group by
		1, 2
)
, all_metrics as (
	select
		l.pool
		, p.blockchain
		, p.project
		, p.fee
		, cast(l.time as date) as time
		, case when l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 then l.token0 else l.token1 end as main_token
		, case when l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 then p0.symbol else p1.symbol end as main_token_symbol
		, case when l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 then l.token1 else l.token0 end as paired_token
		, case when l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 then p1.symbol else p0.symbol end as paired_token_symbol
		, case when l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 then l.amount0 / cast(power(10, p0.decimals) as double) else l.amount1 / cast(power(10, p1.decimals) as double) end as main_token_reserve
		, case when l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 then l.amount1 / cast(power(10, p1.decimals) as double) else l.amount0 / cast(power(10, p0.decimals) as double) end as paired_token_reserve
		, case when l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 then p0.price else p1.price end as main_token_usd_price
		, case when l.token0 = 0xc02fe7317d4eb8753a02c35fe019786854a92001 then p1.price else p0.price end as paired_token_usd_price
		, tv.volume as trading_volume
	from
		{{ ref('lido_liquidity_unichain_stg_uniswap_v4_daily_liquidity') }} as l
	inner join pools as p
		on l.pool = p.pool_id
	left join tokens_prices_daily as p0
		on l.time = p0.time
		and l.token0 = p0.token
	left join tokens_prices_daily as p1
		on l.time = p1.time
		and l.token1 = p1.token
	left join trading_volume as tv
		on l.time = tv.time
		and l.pool = tv.pool
)
select
	am.blockchain || ' ' || am.project || ' ' || coalesce(am.paired_token_symbol, 'unknown') || ':' || coalesce(am.main_token_symbol, 'unknown') || ' ' || format('%,.3f', round(coalesce(am.fee, 0), 4)) as pool_name
	, am.*
from
	all_metrics as am
where
	am.main_token_usd_price is not null