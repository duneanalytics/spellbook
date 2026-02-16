{{ config(
	schema='lido_liquidity_unichain',
	alias='stg_uniswap_v4_modify_liquidity',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['evt_block_date', 'evt_block_time', 'evt_block_number', 'evt_tx_hash', 'evt_index'],
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.evt_block_date')],
) }}

{% set project_start_date = '2025-04-01' %}

select
	ml.id as pool_id
	, ml.evt_block_date
	, ml.evt_block_time
	, ml.evt_block_number
	, ml.evt_tx_hash
	, ml.evt_index
	, ml.liquidityDelta
	, ml.tickLower
	, ml.tickUpper
	, ml.salt
from
	{{ source('uniswap_v4_unichain','PoolManager_evt_ModifyLiquidity') }} as ml
inner join {{ ref('lido_liquidity_unichain_stg_uniswap_v4_pools') }} as p
	on ml.id = p.pool_id
where
	1 = 1
	{% if not is_incremental() -%}
	and ml.evt_block_date >= timestamp '{{ project_start_date }}'
	{% else -%}
	and {{ incremental_predicate('ml.evt_block_date') }}
	{% endif -%}