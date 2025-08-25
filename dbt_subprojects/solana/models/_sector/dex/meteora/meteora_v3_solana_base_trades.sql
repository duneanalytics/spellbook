{# pick up a project_start_date var, with a sensible fallback #}
{% set project_start_date = var('project_start_date', '2025-04-10') %}


{{ 
  config(
    schema='meteora_v3_solana',
    alias='base_trades',
    materialized='incremental',
    file_format='delta',
    partition_by=['block_month'],
    incremental_strategy='merge',
    unique_key=['tx_id','outer_instruction_index','inner_instruction_index','tx_index','block_month']
  ) 
}}

with swap_calls as
(
select 
  call_block_time as block_time
, call_block_slot as block_slot
, call_outer_executing_account as trade_source
, account_token_a_mint as token_a
, account_token_b_mint as token_b
, account_token_a_vault as token_a_vault
, account_token_b_vault as token_b_vault
, call_tx_signer as trader_id
, call_tx_id  as tx_id
, call_outer_instruction_index as outer_instruction_index
, coalesce(call_inner_instruction_index,0) as inner_instruction_index
, call_tx_index as tx_index
, row_number () over (partition by call_tx_id, call_tx_index, call_outer_instruction_index order by coalesce(call_inner_instruction_index,0) asc) as rn
from {{ source ('meteora_solana','cp_amm_call_swap') }} cs
where 1=1
{% if is_incremental() %}
and {{incremental_predicate('call_block_time')}}
{% else %}
and call_block_time > timestamp '{{project_start_date}}'
{% endif %}
),
swap_event_details as 
(

select 
evt_block_time as block_time
, trade_direction
, cast(json_extract(params, '$.SwapParameters.amount_in') as decimal) as token_in_amount_raw
, cast(json_extract(swap_result, '$.SwapResult.output_amount') as decimal) as token_out_amount_raw
, cast(json_extract(swap_result, '$.SwapResult.lp_fee') as decimal) + cast(json_extract(swap_result, '$.SwapResult.protocol_fee') as decimal) + cast(json_extract(swap_result, '$.SwapResult.partner_fee') as decimal) + cast(json_extract(swap_result, '$.SwapResult.referral_fee') as decimal) as total_fees_raw 
, pool as project_program_id
, evt_tx_id  as tx_id
, evt_outer_instruction_index as outer_instruction_index
, coalesce(evt_inner_instruction_index,0) as inner_instruction_index
, evt_tx_index as tx_index
, row_number () over (partition by evt_tx_id, evt_tx_index, evt_outer_instruction_index order by coalesce(evt_inner_instruction_index,0) asc) as rn
from {{ source ('meteora_solana','cp_amm_evt_evtswap') }} es 
where 1=1
{% if is_incremental() %}
and {{incremental_predicate('evt_block_time')}}
{% else %}
and evt_block_time > timestamp '{{project_start_date}}'
{% endif %}

),


swaps_data as (
select 
  'solana' as blockchain
, 'meteora' as project
, 3 as version
, cast (date_trunc('month',sd.block_time) as date) as block_month
, sd.block_time  
, sd.block_slot 
, sd.trade_source
, cast(evt.token_out_amount_raw as uint256) as token_bought_amount_raw
, cast(evt.token_in_amount_raw as uint256) as token_sold_amount_raw
, cast(null as double) as fee_tier
-- , evt.total_fees_raw
, case when evt.trade_direction = 0 then sd.token_a else sd.token_b end as token_sold_mint_address
, case when evt.trade_direction = 1 then sd.token_a else sd.token_b  end as token_bought_mint_address
-- , sd.token_b as token_fee_mint_address
, case when evt.trade_direction = 0 then sd.token_a_vault else sd.token_b_vault end as token_sold_vault
, case when evt.trade_direction = 1 then sd.token_a_vault else sd.token_b_vault  end as token_bought_vault
, evt.project_program_id
, 'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG' as project_main_id
, sd.trader_id
, sd.tx_id
, sd.outer_instruction_index
, sd.inner_instruction_index
, sd.tx_index
from swap_calls sd 
left join swap_event_details evt 
on ( 
    sd.tx_id=evt.tx_id 
    and sd.block_time=evt.block_time
    and sd.tx_index=evt.tx_index 
    and sd.outer_instruction_index = evt.outer_instruction_index
    and sd.rn=evt.rn
)
)
select * from swaps_data 
