{{
	config(
		schema = 'meteora_v4_solana',
		alias = 'base_trades',
		partition_by = ['block_month'],
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    	unique_key = ['block_month', 'surrogate_key']
	)
}}

{% set project_start_date = '2025-04-23 06:13' -%} --grabbed program deployed at time (account created at).
{% set swap_tables = ['dynamic_bonding_curve_call_swap','dynamic_bonding_curve_call_swap2'] -%}


with individual_program_swaps as (
	{% for tbl in swap_tables -%}
	select
		call_block_time as block_time
		, call_block_slot as block_slot
		, call_outer_executing_account as trade_source
		, account_base_mint as token_a
		, account_quote_mint as token_b
		, account_base_vault as token_a_vault
		, account_quote_vault as token_b_vault
		, call_tx_signer as trader_id
		, call_tx_id as tx_id
		, call_outer_instruction_index as outer_instruction_index
		, coalesce(call_inner_instruction_index, 0) as inner_instruction_index
		, call_tx_index as tx_index
		, row_number() over (partition by call_tx_id, call_tx_index, call_outer_instruction_index order by coalesce(call_inner_instruction_index, 0) asc) as rn
	from
		{{ source('meteora_solana',tbl) }} as cs
	where
		1 = 1
		{% if is_incremental() -%}
		and {{incremental_predicate('call_block_time')}}
		{% else -%}
		and call_block_time >= timestamp '{{project_start_date}}'
		{% endif -%}
	{% if not loop.last -%} union all {% endif -%}
	{% endfor -%}
)
, swap_details as (
	select
		*
		, row_number() over (partition by tx_id, outer_instruction_index order by inner_instruction_index) as swap_number
	from
		individual_program_swaps
)
, swaps_v1 as (
	select
		evt_block_time as block_time
		, trade_direction
		, cast(amount_in as double) as token_in_amount_raw
		, cast(json_extract(swap_result, '$.SwapResult.output_amount') as double) as token_out_amount_raw
		, cast(json_extract(swap_result, '$.SwapResult.trading_fee') as double) + cast(json_extract(swap_result, '$.SwapResult.protocol_fee') as double) + cast(json_extract(swap_result, '$.SwapResult.referral_fee') as double) as total_fees_raw
		, pool as project_program_id
		, evt_tx_id as tx_id
		, evt_outer_instruction_index as outer_instruction_index
		, coalesce(evt_inner_instruction_index, 0) as inner_instruction_index
		, evt_tx_index as tx_index
	from
		{{ source('meteora_solana','dynamic_bonding_curve_evt_evtswap') }} as es
	where
		1 = 1
		{% if is_incremental() -%}
		and {{incremental_predicate('evt_block_time')}}
		{% else -%}
		and evt_block_time >= timestamp '{{project_start_date}}'
		{% endif -%}
)
, swaps_v2 as (
	select
		evt_block_time as block_time
		, trade_direction
		, cast(json_extract(swap_result, '$.SwapResult2.included_fee_input_amount') as double) as token_in_amount_raw
		, cast(json_extract(swap_result, '$.SwapResult2.output_amount') as double) as token_out_amount_raw
		, cast(json_extract(swap_result, '$.SwapResult2.trading_fee') as double) + cast(json_extract(swap_result, '$.SwapResult.protocol_fee') as double) + cast(json_extract(swap_result, '$.SwapResult.referral_fee') as double) as total_fees_raw
		, pool as project_program_id
		, evt_tx_id as tx_id
		, evt_outer_instruction_index as outer_instruction_index
		, coalesce(evt_inner_instruction_index, 0) as inner_instruction_index
		, evt_tx_index as tx_index
	from
		{{ source('meteora_solana','dynamic_bonding_curve_evt_evtswap2') }} as es
	where
		1 = 1
		{% if is_incremental() -%}
		and {{incremental_predicate('evt_block_time')}}
		{% else -%}
		and evt_block_time >= timestamp '{{project_start_date}}'
		{% endif -%}
)
, combined as (
	select * from swaps_v1
	union all
	select * from swaps_v2
)
, evt_deatils as (
	select
		*
		, row_number() over (partition by tx_id, tx_index, outer_instruction_index order by coalesce(inner_instruction_index, 0) asc) as rn
	from
		combined
)
, swaps_table as (
	select
		'solana' as blockchain
		, 'meteora' as project
		, 4 as version
    	, 'dbc' as version_name
		, cast(date_trunc('month', sd.block_time) as date) as block_month
		, sd.block_time
		, sd.block_slot
		, sd.trade_source
		, cast(evt.token_out_amount_raw as uint256) as token_bought_amount_raw
		, cast(evt.token_in_amount_raw as uint256) as token_sold_amount_raw
		, cast(null as double) as fee_tier
		-- , evt.total_fees_raw
		, case when evt.trade_direction = 0 then sd.token_a else sd.token_b end as token_sold_mint_address
		, case when evt.trade_direction = 1 then sd.token_a else sd.token_b end as token_bought_mint_address
		-- , sd.token_b as token_fee_mint_address
		, case when evt.trade_direction = 0 then sd.token_a_vault else sd.token_b_vault end as token_sold_vault
		, case when evt.trade_direction = 1 then sd.token_a_vault else sd.token_b_vault end as token_bought_vault
		, evt.project_program_id
		, 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN' as project_main_id
		, sd.trader_id
		, sd.tx_id
		, sd.outer_instruction_index
		, sd.inner_instruction_index
		, sd.tx_index
    	, {{ dbt_utils.generate_surrogate_key(['sd.tx_id', 'sd.tx_index', 'sd.outer_instruction_index', 'sd.inner_instruction_index']) }} as surrogate_key
	from
		swap_details as sd
	left join evt_deatils as evt
		on sd.tx_id = evt.tx_id
		and sd.block_time = evt.block_time
		and sd.outer_instruction_index = evt.outer_instruction_index
		and sd.rn = evt.rn
)
select * from swaps_table