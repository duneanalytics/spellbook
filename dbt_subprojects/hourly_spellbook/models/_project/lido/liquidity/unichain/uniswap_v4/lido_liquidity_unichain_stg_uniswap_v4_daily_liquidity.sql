{{ config(
	schema='lido_liquidity_unichain',
	alias='stg_uniswap_v4_daily_liquidity',
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
, ml as (
	select
		ml.*
		, p.token0
		, p.token1
		, p.init_evt_block_time
		, p.init_sqrtPriceX96
	from
		{{ ref('lido_liquidity_unichain_stg_uniswap_v4_modify_liquidity') }} as ml
	inner join pools as p
		on ml.pool_id = p.pool_id
	where
		1 = 1
		{% if not is_incremental() -%}
		and ml.evt_block_date >= timestamp '{{ project_start_date }}'
		{% else -%}
		and {{ incremental_predicate('ml.evt_block_date') }}
		{% endif -%}
)
, swaps as (
	select
		swaps.pool_id
		, swaps.evt_block_number
		, swaps.evt_block_time
		, swaps.sqrtPriceX96
		, swaps.amount0
		, swaps.amount1
		, swaps.evt_tx_hash
		, swaps.evt_index
	from
		{{ ref('lido_liquidity_unichain_stg_uniswap_v4_swaps') }} as swaps
	where
		swaps.evt_block_date >= timestamp '{{ project_start_date }}'
)
, ml_with_price as (
	select
		ml.*
		, coalesce(s.evt_block_time, ml.init_evt_block_time) as most_recent_time
		, coalesce(s.sqrtPriceX96, ml.init_sqrtPriceX96) as sqrtPriceX96
	from
		ml as ml
	asof left join swaps as s
		on s.pool_id = ml.pool_id
		and s.evt_block_time <= ml.evt_block_time
)
, prep_for_calculations as (
	select
		mwp.evt_block_time
		, mwp.evt_block_number
		, mwp.pool_id as id
		, mwp.evt_tx_hash
		, mwp.evt_index
		, mwp.salt
		, mwp.token0
		, mwp.token1
		, log(mwp.sqrtPriceX96/power(2, 96), 10)/log(1.0001, 10) as tickCurrent
		, mwp.tickLower
		, mwp.tickUpper
		, sqrt(power(1.0001, mwp.tickLower)) as sqrtRatioL
		, sqrt(power(1.0001, mwp.tickUpper)) as sqrtRatioU
		, mwp.sqrtPriceX96/power(2, 96) as sqrtPrice
		, mwp.sqrtPriceX96
		, mwp.liquidityDelta
	from
		ml_with_price as mwp
)
, base_liquidity_amounts as (
	select
		pfc.evt_block_time
		, pfc.evt_block_number
		, pfc.id
		, pfc.evt_tx_hash
		, pfc.evt_index
		, pfc.salt
		, pfc.token0
		, pfc.token1
		, case
			when pfc.sqrtPrice <= pfc.sqrtRatioL then pfc.liquidityDelta * ((pfc.sqrtRatioU - pfc.sqrtRatioL)/(pfc.sqrtRatioL*pfc.sqrtRatioU))
			when pfc.sqrtPrice >= pfc.sqrtRatioU then 0
			else pfc.liquidityDelta * ((pfc.sqrtRatioU - pfc.sqrtPrice)/(pfc.sqrtPrice*pfc.sqrtRatioU))
			end as amount0
		, case
			when pfc.sqrtPrice <= pfc.sqrtRatioL then 0
			when pfc.sqrtPrice >= pfc.sqrtRatioU then pfc.liquidityDelta*(pfc.sqrtRatioU - pfc.sqrtRatioL)
			else pfc.liquidityDelta*(pfc.sqrtPrice - pfc.sqrtRatioL)
			end as amount1
	from
		prep_for_calculations as pfc
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
		s.pool_id as pool
		, date_trunc('minute', s.evt_block_time) as minute
		, s.evt_tx_hash
		, s.evt_index
		, p.token0
		, p.token1
		, -1 * s.amount0 as amount0
		, -1 * s.amount1 as amount1
	from
		{{ ref('lido_liquidity_unichain_stg_uniswap_v4_swaps') }} as s
	inner join pools as p
		on s.pool_id = p.pool_id
	where
		1 = 1
		{% if not is_incremental() -%}
		and s.evt_block_date >= timestamp '{{ project_start_date }}'
		{% else -%}
		and {{ incremental_predicate('s.evt_block_date') }}
		{% endif -%}
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
select
	pl.*
from
	pools_liquidity as pl