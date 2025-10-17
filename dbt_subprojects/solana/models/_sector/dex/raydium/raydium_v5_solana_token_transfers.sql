{{
  config(
    schema = 'raydium_v5_solana'
    , alias = 'token_transfers'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_date', 'unique_instruction_key']
  )
}}

{% set project_start_date = '2024-05-16' %}

-- Base swaps from raydium v5
with raydium_swaps as (
	select distinct
		date_trunc('day', call_block_time) as block_date
		, call_block_slot as block_slot
		, call_tx_index as tx_index
		, call_outer_instruction_index as outer_instruction_index
	from
		{{ source('raydium_cp_solana', 'raydium_cp_swap_call_swapBaseOutput') }}
	where
		1=1
		{% if is_incremental() or true -%}
		and {{incremental_predicate('call_block_time')}}
		{% else -%}
		and call_block_date >= date '{{project_start_date}}'
		{% endif -%}
	union all
	select distinct
		date_trunc('day', call_block_time) as block_date
		, call_block_slot as block_slot
		, call_tx_index as tx_index
		, call_outer_instruction_index as outer_instruction_index
	from
		{{ source('raydium_cp_solana', 'raydium_cp_swap_call_swapBaseInput') }}
	where
		1=1
		{% if is_incremental() or true -%}
		and {{incremental_predicate('call_block_time')}}
		{% else -%}
		and call_block_date >= date '{{project_start_date}}'
		{% endif -%}
)
, token_transfers as (
	select
		*
	from
		{{ source('tokens_solana','transfers') }}
	where
		1=1
		and token_version != 'native'
		{% if is_incremental() or true %}
		and {{ incremental_predicate('block_time') }}
		{% else %}
		and block_time >= timestamp '{{ project_start_date }}'
		{% endif %}
)

select
	*
from
	token_transfers
inner join raydium_swaps using (block_date, block_slot, tx_index, outer_instruction_index)