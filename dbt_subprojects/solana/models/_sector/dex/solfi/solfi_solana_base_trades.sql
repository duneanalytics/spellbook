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
  direction as trade_source,
  amountIn as token_sold_amount_raw,
  null as token_bought_amount_raw, -- update once we have this field
  cast(null as double) as fee_tier,
  account_userTokenAccountA as token_sold_mint_address,
  account_userTokenAccountB as token_bought_mint_address,
  account_poolTokenAccountA as token_sold_vault,
  account_poolTokenAccountB as token_bought_vault,
  account_pair as project_program_id,
  call_outer_executing_account as project_main_id,
  trader_id,
  tx_id,
  outer_instruction_index,
  inner_instruction_index,
  tx_index
from solfi_swaps