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
, ml_batch as (
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
, swaps_all as (
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
, ml_enriched as (
	select
		ml_batch.*
		, coalesce(sw.sqrtPriceX96, ml_batch.init_sqrtPriceX96) as sqrtPriceX96
	from
		ml_batch
	asof left join swaps_all as sw
		on sw.pool_id = ml_batch.pool_id
		and sw.evt_block_time <= ml_batch.evt_block_time
)
, ml_amounts as (
	select
		me.evt_block_time
		, me.evt_block_number
		, me.pool_id
		, me.evt_tx_hash
		, me.evt_index
		, me.salt
		, me.token0
		, me.token1
		, me.tickLower
		, me.tickUpper
		, me.liquidityDelta
		, me.amount0
		, me.amount1
	from
		ml_enriched as me
)
, minute_changes as (
	select
		ma.pool_id as pool
		, date_trunc('minute', ma.evt_block_time) as minute
		, ma.evt_tx_hash
		, ma.evt_index
		, ma.token0
		, ma.token1
		, ma.amount0
		, ma.amount1
	from
		ml_amounts as ma

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
select
	date_trunc('day', mc.minute) as time
	, mc.pool
	, mc.token0
	, mc.token1
	, sum(mc.amount0) as amount0
	, sum(mc.amount1) as amount1
from
	minute_changes as mc
group by
	1, 2, 3, 4
