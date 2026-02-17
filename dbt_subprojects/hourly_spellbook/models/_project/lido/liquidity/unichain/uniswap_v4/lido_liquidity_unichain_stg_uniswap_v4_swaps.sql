{{ config(
	schema='lido_liquidity_unichain',
	alias='stg_uniswap_v4_swaps',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['evt_block_date', 'evt_block_time', 'evt_block_number', 'evt_tx_hash', 'evt_index'],
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
) }}

{% set project_start_date = '2025-04-01' %}

select
	s.id as pool_id
	, s.contract_address
	, s.evt_tx_hash
	, s.evt_tx_from
	, s.evt_tx_to
	, s.evt_tx_index
	, s.evt_index
	, s.evt_block_time
	, s.evt_block_number
	, s.evt_block_date
	, s.amount0
	, s.amount1
	, s.fee
	, s.liquidity
	, s.sender
	, s.sqrtPriceX96
	, s.tick
from
	{{ source('uniswap_v4_unichain','PoolManager_evt_Swap') }} as s
inner join {{ ref('lido_liquidity_unichain_stg_uniswap_v4_pools') }} as p
	on s.id = p.pool_id
where
	1 = 1
	{% if not is_incremental() -%}
	and s.evt_block_time >= timestamp '{{ project_start_date }}'
	{% else -%}
	and {{ incremental_predicate('s.evt_block_time') }}
	{% endif -%}