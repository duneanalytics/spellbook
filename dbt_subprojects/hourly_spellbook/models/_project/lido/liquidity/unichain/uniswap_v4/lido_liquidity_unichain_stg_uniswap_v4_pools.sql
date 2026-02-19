{{ config(
	schema='lido_liquidity_unichain',
	alias='stg_uniswap_v4_pools',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['pool_id'],
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
		, evt_block_time as init_evt_block_time
		, sqrtPriceX96 as init_sqrtPriceX96
	from
		{{ source('uniswap_v4_unichain','PoolManager_evt_Initialize') }}
	where
		(
            currency0 = 0xc02fE7317D4eb8753a02c35fe019786854A92001
		    or currency1 = 0xc02fE7317D4eb8753a02c35fe019786854A92001
        )
        {% if not is_incremental() -%}
        and evt_block_time >= timestamp '{{ project_start_date }}'
        {% else -%}
        and {{ incremental_predicate('evt_block_time') }}
        {% endif -%}
)
select *
from pools