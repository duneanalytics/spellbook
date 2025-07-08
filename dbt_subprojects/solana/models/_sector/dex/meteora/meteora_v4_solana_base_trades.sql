-- Base trades for dynamic bonding curve of meteora
select 
'solana' as blockchain
, 'meteora' as project
, 4 as version
, cast (date_trunc('month',cs.call_block_time) as date) as block_month
, cs.call_block_time as block_time
, cs.call_block_slot as block_slot
, cs.call_outer_executing_account as trade_source
, es.trade_direction
, json_extract(es.swap_result, '$.SwapResult.actual_input_amount') as token_bought_amount_raw
, json_extract(es.swap_result, '$.SwapResult.output_amount') as token_sold_amount_raw
, cast(null as double) as fee_tier
, cs.account_base_mint as token_bought_address
, cs.account_quote_mint as token_sold_address
, cs.account_base_vault as token_bought_vault
, cs.account_quote_vault as token_sold_vault
, es.pool as project_program_id
, 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN' as project_main_id
, cs.call_tx_signer as trader_id
, call_tx_id  as tx_id
, call_outer_instruction_index as outer_instruction_index
, coalesce(call_inner_instruction_index,0) as inner_instruction_index
, call_tx_index as tx_index



from meteora_solana.dynamic_bonding_curve_call_swap cs
left join  meteora_solana.dynamic_bonding_curve_evt_evtswap es 
on ( 
    cs.call_tx_id=es.evt_tx_id 
    and cs.call_block_time=es.evt_block_time 
    and cs.call_outer_instruction_index = es.evt_outer_instruction_index
    )
where cs.call_tx_id = '5MPByPzLAnzcefsZbfwNibqY746emuArMeUUVFFEMVjKrzrKvtBsvfzT3y9bqFJssimKTUcXGLSW2UWpbwPK65s4'
and cs.call_block_time > timestamp '2025-07-08 04:20'
