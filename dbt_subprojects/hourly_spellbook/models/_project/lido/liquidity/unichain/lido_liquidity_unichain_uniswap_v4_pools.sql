{{ config(
	schema='lido_liquidity_unichain',
	alias='uniswap_v4_pools',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['pool', 'time'],
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.time')],
	post_hook='{{ hide_spells() }}',
) }}

{% set project_start_date = '2025-04-01' %}

with pools as (
	select
		id as pool_id
		, 'unichain' as blockchain
		, 'uniswap_v4' as project
		, currency0 as token0
		, currency1 as token1
		, cast(fee as double)/10000 as fee
	from
		{{ source('uniswap_v4_unichain','PoolManager_evt_Initialize') }}
	where
		currency0 = 0xc02fE7317D4eb8753a02c35fe019786854A92001
		or currency1 = 0xc02fE7317D4eb8753a02c35fe019786854A92001
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
	select distinct
		date_trunc('day', minute) as time
		, if(contract_address = 0x4200000000000000000000000000000000000006, 0x0000000000000000000000000000000000000000, contract_address) as token
		, decimals
		, if(symbol = 'WETH', 'ETH', symbol) as symbol
		, avg(price) as price
	from
		{{ source('prices','usd') }}
	{% if not is_incremental() %}
	where date_trunc('day', minute) >= date '{{ project_start_date }}'
	{% else %}
	where {{ incremental_predicate('minute') }}
	{% endif %}
		and date_trunc('day', minute) < current_date
		and blockchain = 'unichain'
		and contract_address in (select address from tokens)
	group by
		1, 2, 3, 4

	union all

	select distinct
		date_trunc('day', minute)
		, if(contract_address = 0x4200000000000000000000000000000000000006, 0x0000000000000000000000000000000000000000, contract_address) as token
		, decimals
		, if(symbol = 'WETH', 'ETH', symbol) as symbol
		, last_value(price) over (partition by date_trunc('day', minute), contract_address order by minute nulls first range between unbounded preceding and unbounded following) as price
	from
		{{ source('prices','usd') }}
	where
		date_trunc('day', minute) = current_date
		and blockchain = 'unichain'
		and contract_address in (select address from tokens)
)
, tokens_prices_hourly as (
	select distinct
		date_trunc('hour', minute) as time
		, lead(date_trunc('hour', minute), 1, date_trunc('hour', now() + interval '1' hour)) over (partition by contract_address order by date_trunc('hour', minute) nulls first) as next_time
		, if(contract_address = 0x4200000000000000000000000000000000000006, 0x0000000000000000000000000000000000000000, contract_address) as token
		, decimals
		, if(symbol = 'WETH', 'ETH', symbol) as symbol
		, last_value(price) over (partition by date_trunc('hour', minute), contract_address order by minute nulls first range between unbounded preceding and unbounded following) as price
	from
		{{ source('prices','usd') }}
	{% if not is_incremental() %}
	where date_trunc('day', minute) >= date '{{ project_start_date }}'
	{% else %}
	where {{ incremental_predicate('minute') }}
	{% endif %}
		and blockchain = 'unichain'
		and contract_address in (select address from tokens)
)
, get_recent_sqrtPriceX96 as (
	select
		tbl.*
	from
		(
			select
				ml.*
				, i.currency0 as token0
				, i.currency1 as token1
				, coalesce(s.evt_block_time, i.evt_block_time) as most_recent_time
				, coalesce(s.sqrtPriceX96, i.sqrtPriceX96) as sqrtPriceX96
				, row_number() over (partition by ml.id, ml.evt_block_time, ml.evt_index order by case when s.sqrtPriceX96 is not null then s.evt_block_time else i.evt_block_time end desc) as rn
			from
				{{ source('uniswap_v4_unichain','PoolManager_evt_ModifyLiquidity') }} as ml
			join pools as p
				on ml.id = p.pool_id
			left join {{ source('uniswap_v4_unichain','PoolManager_evt_Swap') }} as s
				on ml.evt_block_time > s.evt_block_time
				and ml.id = s.id
			left join {{ source('uniswap_v4_unichain','PoolManager_evt_Initialize') }} as i
				on ml.evt_block_time >= i.evt_block_time
				and i.id = ml.id
		) as tbl
	where
		tbl.rn = 1
)
, prep_for_calculations as (
	select
		g.evt_block_time
		, g.evt_block_number
		, g.id
		, g.evt_tx_hash
		, g.evt_index
		, g.salt
		, g.token0
		, g.token1
		, log(g.sqrtPriceX96/power(2, 96), 10)/log(1.0001, 10) as tickCurrent
		, g.tickLower
		, g.tickUpper
		, sqrt(power(1.0001, g.tickLower)) as sqrtRatioL
		, sqrt(power(1.0001, g.tickUpper)) as sqrtRatioU
		, g.sqrtPriceX96/power(2, 96) as sqrtPrice
		, g.sqrtPriceX96
		, g.liquidityDelta
	from
		get_recent_sqrtPriceX96 as g
)

, base_liquidity_amounts as (
	select
		pc.evt_block_time
		, pc.evt_block_number
		, pc.id
		, pc.evt_tx_hash
		, pc.evt_index
		, pc.salt
		, pc.token0
		, pc.token1
		, case when pc.sqrtPrice <= pc.sqrtRatioL then pc.liquidityDelta * ((pc.sqrtRatioU - pc.sqrtRatioL)/(pc.sqrtRatioL*pc.sqrtRatioU))
			when pc.sqrtPrice >= pc.sqrtRatioU then 0
			else pc.liquidityDelta * ((pc.sqrtRatioU - pc.sqrtPrice)/(pc.sqrtPrice*pc.sqrtRatioU))
			end as amount0
		, case when pc.sqrtPrice <= pc.sqrtRatioL then 0
			when pc.sqrtPrice >= pc.sqrtRatioU then pc.liquidityDelta*(pc.sqrtRatioU - pc.sqrtRatioL)
			else pc.liquidityDelta*(pc.sqrtPrice - pc.sqrtRatioL)
			end as amount1
	from
		prep_for_calculations as pc
)

, liquidity_change_base as (
	select
		b.id as pool
		, date_trunc('minute', b.evt_block_time) as minute
		, b.evt_tx_hash
		, b.evt_index
		, b.token0
		, b.token1
		, b.amount0
		, b.amount1
	from
		base_liquidity_amounts as b

	union all

	select
		s.id as pool
		, date_trunc('minute', s.evt_block_time) as minute
		, s.evt_tx_hash
		, s.evt_index
		, p.token0
		, p.token1
		, -1 * s.amount0
		, -1 * s.amount1
	from
		{{ source('uniswap_v4_unichain','PoolManager_evt_Swap') }} as s
	join pools as p
		on p.pool_id = s.id
)

, pools_liquidity as (
	select
		date_trunc('day', lcb.minute) as time
		, lcb.pool
		, lcb.token0
		, lcb.token1
		, sum(lcb.amount0) as amount0
		, sum(lcb.amount1) as amount1
	from
		liquidity_change_base as lcb
	group by
		1, 2, 3, 4
)

, swap_events_hourly as (
	select
		sw.evt_block_time as time
		, sw.id as pool
		, p.token0
		, p.token1
		, coalesce(sum(cast(abs(sw.amount0) as double)), 0) as amount0
		, coalesce(sum(cast(abs(sw.amount1) as double)), 0) as amount1
	from
		{{ source('uniswap_v4_unichain','PoolManager_evt_Swap') }} as sw
	inner join pools as p
		on sw.id = p.pool_id
	{% if not is_incremental() %}
	where date_trunc('day', sw.evt_block_time) >= date '{{ project_start_date }}'
	{% else %}
	where {{ incremental_predicate('sw.evt_block_time') }}
	{% endif %}
	group by
		1, 2, 3, 4
)
, trading_volume as (
	select
		date_trunc('day', s.time) as time
		, s.pool
		, sum(case when p0.decimals is not null then coalesce((p0.price * s.amount0) / cast(power(10, p0.decimals) as double), 0)
			else coalesce((p1.price * s.amount1) / cast(power(10, p1.decimals) as double), 0)
			end) as volume
	from
		swap_events_hourly as s
	left join tokens_prices_hourly as p0
		on date_trunc('hour', s.time) >= p0.time
		and date_trunc('hour', s.time) < p0.next_time
		and s.token0 = p0.token
	left join tokens_prices_hourly as p1
		on date_trunc('hour', s.time) >= p1.time
		and date_trunc('hour', s.time) < p1.next_time
		and s.token1 = p1.token
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
		pools_liquidity as l
	left join pools as p
		on l.pool = p.pool_id
	left join tokens as t0
		on l.token0 = t0.address
	left join tokens as t1
		on l.token1 = t1.address
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
	blockchain || ' ' || project || ' ' || coalesce(paired_token_symbol, 'unknown') || ':' || coalesce(main_token_symbol, 'unknown') || ' ' || format('%,.3f', round(coalesce(fee, 0), 4)) as pool_name
	, am.*
from
	all_metrics as am
where
	am.main_token_usd_price is not null
