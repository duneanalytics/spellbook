{{
  config(
    schema = 'raydium_v5_solana'
    , alias = 'stg_decoded_swaps'
    , partition_by = ['call_block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.call_block_time')]
    , unique_key = ['call_block_month', 'unique_instruction_key']
  )
}}

{% set project_start_date = '2024-05-16' %}

with swaps as (
	select
		account_poolState
		, call_is_inner
		, call_outer_instruction_index
		, call_inner_instruction_index
		, call_tx_id
		, call_block_time
		, call_block_slot
		, call_block_date
		, call_outer_executing_account
		, call_tx_signer
		, call_tx_index
		, account_inputTokenMint
		, account_outputTokenMint
		, account_inputVault
		, account_outputVault
	from
		{{ source('raydium_cp_solana', 'raydium_cp_swap_call_swapBaseOutput') }}
	where
		1=1
		{% if is_incremental() -%}
		and {{incremental_predicate('call_block_time')}}
		{% else -%}
		and call_block_date >= date '{{project_start_date}}'
		{% endif -%}
	union all
	select
		account_poolState
		, call_is_inner
		, call_outer_instruction_index
		, call_inner_instruction_index
		, call_tx_id
		, call_block_time
		, call_block_slot
		, call_block_date
		, call_outer_executing_account
		, call_tx_signer
		, call_tx_index
		, account_inputTokenMint
		, account_outputTokenMint
		, account_inputVault
		, account_outputVault
	from
		{{ source('raydium_cp_solana', 'raydium_cp_swap_call_swapBaseInput') }}
	where
		1=1
		{% if is_incremental() -%}
		and {{incremental_predicate('call_block_time')}}
		{% else -%}
		and call_block_date >= date '{{project_start_date}}'
		{% endif -%}
)

select
	*
	, cast(date_trunc('month', call_block_date) as date) as call_block_month
	, {{ dbt_utils.generate_surrogate_key(['call_block_slot', 'call_tx_id', 'call_tx_index', 'call_outer_instruction_index', 'call_inner_instruction_index']) }} as unique_instruction_key
from
	swaps