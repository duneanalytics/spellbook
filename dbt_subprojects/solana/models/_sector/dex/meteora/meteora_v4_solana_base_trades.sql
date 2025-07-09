with swap_details as
(
select 
'solana' as blockchain
, 'meteora' as project
, 4 as version
, cast (date_trunc('month',call_block_time) as date) as block_month
, call_block_time as block_time
, call_block_slot as block_slot
, call_outer_executing_account as trade_source
, account_base_mint as token_a
, account_quote_mint as token_b
, account_base_vault as token_a_vault
, account_quote_vault as token_b_vault
, 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN' as project_main_id
, call_tx_signer as trader_id
, call_tx_id  as tx_id
, call_outer_instruction_index as outer_instruction_index
, coalesce(call_inner_instruction_index,0) as inner_instruction_index
, call_tx_index as tx_index
, row_number () over (partition by call_tx_index, call_outer_instruction_index order by call_inner_instruction_index asc) as rn
from meteora_solana.dynamic_bonding_curve_call_swap cs
where call_tx_id = '3Uj7Eh6WcH6VsmwPMC47wWkHKSXBWYoeTtp49hSoUhKRrmj9bGD9oGK7WKZLdqzxxSX449R1zP29gS2kTDqx6JLY'
and call_block_time > timestamp '2025-07-08 04:20'
),
evt_deatils as 
(

select 


evt_block_time as block_time
, trade_direction
, json_extract(swap_result, '$.SwapResult.actual_input_amount') as token_in_amount_raw
, json_extract(swap_result, '$.SwapResult.output_amount') as token_out_amount_raw
, cast(null as double) as fee_tier
, pool as project_program_id
, evt_tx_id  as tx_id
, evt_outer_instruction_index as outer_instruction_index
, coalesce(evt_inner_instruction_index,0) as inner_instruction_index
, evt_tx_index as tx_index
, row_number () over (partition by evt_tx_index, evt_outer_instruction_index order by evt_inner_instruction_index asc) as rn
from meteora_solana.dynamic_bonding_curve_evt_evtswap es 
where evt_tx_id = '3Uj7Eh6WcH6VsmwPMC47wWkHKSXBWYoeTtp49hSoUhKRrmj9bGD9oGK7WKZLdqzxxSX449R1zP29gS2kTDqx6JLY'
and evt_block_time > timestamp '2025-07-08 04:20'

)
select sd.*
, case when evt.trade_direction =0 then sd.token_a else sd.token_b end as token_sold_mint_address
, case when evt.trade_direction =1 then sd.token_a else sd.token_b  end as token_bought_mint_address
, evt.token_in_amount_raw as token_sold_amount_raw
, evt.token_out_amount_raw as token_bought_amount_raw
, evt.fee_tier
, evt.project_program_id
from swap_details sd 
left join evt_deatils evt 
on ( 
    sd.tx_id=evt.tx_id 
    and sd.block_time=evt.block_time 
    and sd.outer_instruction_index = evt.outer_instruction_index
    and sd.rn=evt.rn
)
