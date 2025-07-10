with swap_details as
(
select 
  call_block_time as block_time
, call_block_slot as block_slot
, call_outer_executing_account as trade_source
, account_base_mint as token_a
, account_quote_mint as token_b
, account_base_vault as token_a_vault
, account_quote_vault as token_b_vault
, call_tx_signer as trader_id
, call_tx_id  as tx_id
, call_outer_instruction_index as outer_instruction_index
, coalesce(call_inner_instruction_index,0) as inner_instruction_index
, call_tx_index as tx_index
, row_number () over (partition by call_tx_id, call_tx_index, call_outer_instruction_index order by coalesce(call_inner_instruction_index,0) asc) as rn
from meteora_solana.dynamic_bonding_curve_call_swap cs
where 1=1
-- and call_tx_id = '3Uj7Eh6WcH6VsmwPMC47wWkHKSXBWYoeTtp49hSoUhKRrmj9bGD9oGK7WKZLdqzxxSX449R1zP29gS2kTDqx6JLY'
and call_block_time > timestamp '2025-05-09 04:20'
-- and call_tx_signer = '8xefbTCwb9fZbnffTuMaAL99VKKbMMKcgHrgyoYEPX2w'
),
evt_deatils as 
(

select 
evt_block_time as block_time
, trade_direction
, json_extract(swap_result, '$.SwapResult.actual_input_amount') as token_in_amount_raw
, json_extract(swap_result, '$.SwapResult.output_amount') as token_out_amount_raw
, cast(json_extract(swap_result, '$.SwapResult.trading_fee') as double) + cast(json_extract(swap_result, '$.SwapResult.protocol_fee') as double) + cast(json_extract(swap_result, '$.SwapResult.referral_fee') as double) as total_fees_raw 
, pool as project_program_id
, evt_tx_id  as tx_id
, evt_outer_instruction_index as outer_instruction_index
, coalesce(evt_inner_instruction_index,0) as inner_instruction_index
, evt_tx_index as tx_index
, row_number () over (partition by evt_tx_id, evt_tx_index, evt_outer_instruction_index order by coalesce(evt_inner_instruction_index,0) asc) as rn
from meteora_solana.dynamic_bonding_curve_evt_evtswap es 
where 1=1
-- and evt_tx_id = '3Uj7Eh6WcH6VsmwPMC47wWkHKSXBWYoeTtp49hSoUhKRrmj9bGD9oGK7WKZLdqzxxSX449R1zP29gS2kTDqx6JLY'
-- and evt_tx_signer =  '8xefbTCwb9fZbnffTuMaAL99VKKbMMKcgHrgyoYEPX2w'
and evt_block_time > timestamp '2025-05-09 04:20'

),
temp as (
select 
  'solana' as blockchain
, 'meteora' as project
, 4 as version
, cast (date_trunc('month',sd.block_time) as date) as block_month
, sd.block_time 
, sd.block_slot 
, sd.trade_source
, evt.token_out_amount_raw as token_bought_amount_raw
, evt.token_in_amount_raw as token_sold_amount_raw
, evt.total_fees_raw
, case when evt.trade_direction =0 then sd.token_a else sd.token_b end as token_sold_mint_address
, case when evt.trade_direction =1 then sd.token_a else sd.token_b  end as token_bought_mint_address
, sd.token_b as token_fee_mint_address
, case when evt.trade_direction =0 then sd.token_a_vault else sd.token_b_vault end as token_sold_vault
, case when evt.trade_direction =1 then sd.token_a_vault else sd.token_b_vault  end as token_bought_vault
, evt.project_program_id
, 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN' as project_main_id
, sd.trader_id
, sd.tx_id
, sd.outer_instruction_index
, sd.inner_instruction_index
, sd.tx_index
from swap_details sd 
left join evt_deatils evt 
on ( 
    sd.tx_id=evt.tx_id 
    and sd.block_time=evt.block_time 
    and sd.outer_instruction_index = evt.outer_instruction_index
    and sd.rn=evt.rn
)
)
select * from temp 
-- where token_bought_amount_raw is null 
limit 5