{{
  config(
    schema='solfi_solana',
    alias='base_trades',
    materialized='incremental',
    file_format='delta',
    partition_by=['block_month'],
    incremental_strategy='merge',
    unique_key=['tx_id','outer_instruction_index','inner_instruction_index','tx_index','block_month']
  )
}}

{% set project_start_date = var('project_start_date', '2025-08-01') %}

with solfi_swaps as (
  select
    call_block_slot as block_slot,
    call_block_time as block_time,
    cast(date_trunc('month', call_block_time) as date) as block_month,
    coalesce(call_tx_index,0) as tx_index,
    coalesce(call_outer_instruction_index,0) as outer_instruction_index,
    coalesce(call_inner_instruction_index,0) as inner_instruction_index,
    call_tx_id as tx_id,
    call_tx_signer as trader_id,
    call_outer_executing_account as outer_executing_account,
    call_inner_executing_account as inner_executing_account,
    call_is_inner as is_inner_swap,
    call_program_name as program_name,
    call_instruction_name as instruction_name,
    call_version as version,
    account_pair,
    account_poolTokenAccountA,
    account_poolTokenAccountB,
    account_sysvarInstructions,
    account_tokenProgram,
    account_user,
    account_userTokenAccountA,
    account_userTokenAccountB,
    amountIn,
    direction
  from {{ source('solfi_solana', 'solfi_call_swap') }}
  where 1=1
    {% if is_incremental() %}
      and {{ incremental_predicate('call_block_time') }}
    {% else %}
      and call_block_time >= timestamp '{{ project_start_date }}'
    {% endif %}
)
select
  'solana' as blockchain,
  'solfi' as project,
  1 as version,
  block_month,
  block_time,
  block_slot,
  case when is_inner_swap = false then 'direct' else outer_executing_account end as trade_source,
  amountIn as token_sold_amount_raw,
  cast(null as double) as token_bought_amount_raw, -- update once we have this field
  cast(null as double) as fee_tier,
  case when direction = 1 then account_userTokenAccountA else account_userTokenAccountB end as token_sold_mint_address,
  case when direction = 1 then account_userTokenAccountB else account_userTokenAccountA end as token_bought_mint_address,
  case when direction = 1 then account_poolTokenAccountA else account_poolTokenAccountB end as token_sold_vault,
  case when direction = 1 then account_poolTokenAccountB else account_poolTokenAccountA end as token_bought_vault,
  account_pair as project_program_id,
  outer_executing_account as project_main_id,
  trader_id,
  tx_id,
  outer_instruction_index,
  inner_instruction_index,
  tx_index
from solfi_swaps