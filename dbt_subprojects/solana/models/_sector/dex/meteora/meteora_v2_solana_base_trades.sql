{# pick up a project_start_date var, with a sensible fallback #}
{% set project_start_date = var('project_start_date', '2023-11-07') %}

{# list of your swap tables to loop over #}
{% set swap_tables = ['lb_clmm_call_swap', 'lb_clmm_call_swap2','lb_clmm_call_swapexactout','lb_clmm_call_swapexactout2','lb_clmm_call_swapwithpriceimpact','lb_clmm_call_swapwithpriceimpact2'] %}

{{ 
  config(
    schema='meteora_v2_solana',
    alias='base_trades',
    materialized='incremental',
    file_format='delta',
    partition_by=['block_month'],
    incremental_strategy='merge',
    unique_key=['tx_id','outer_instruction_index','inner_instruction_index','tx_index','block_month']
  ) 
}}

with

all_swaps as (
  with individual_program_swaps as 
  (
  {% for tbl in swap_tables %}
    select
      call_block_slot as block_slot,
      call_block_time as block_time,
      coalesce(call_tx_index,0) as tx_index,
      coalesce(call_outer_instruction_index,0) as outer_instruction_index,
      coalesce(call_inner_instruction_index,0) as inner_instruction_index,
      call_tx_id as tx_id,
      call_tx_signer as trader_id,
      call_outer_executing_account as outer_executing_account,
      account_tokenXMint as account_tokenXMint,
      account_tokenYMint as account_tokenYMint,
      account_reserveX as account_reserveX,
      account_reserveY as account_reserveY,
      account_lbPair as account_lbPair,
      call_is_inner as is_inner_swap
    from {{ source('dlmm_solana', tbl) }}
    where 1=1
      {% if is_incremental() %}
        and {{ incremental_predicate('call_block_time') }}
      {% else %}
        and call_block_time >= timestamp '{{ project_start_date }}'
      {% endif %}
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
  )
  select *,       
      row_number() over (
        partition by tx_id, outer_instruction_index
        order by inner_instruction_index
      ) as swap_number
  from individual_program_swaps
),

inner_instruct as (
  select
    tx_id,
    block_time,
    coalesce(tx_index,0) as tx_index,
    coalesce(outer_instruction_index,0) as outer_instruction_index,
    coalesce(inner_instruction_index,0) as inner_instruction_index,
    row_number() over ( partition by tx_id, outer_instruction_index order by inner_instruction_index ) as swap_number,
    data
  from {{ source('solana','instruction_calls') }}
  where tx_success
    and is_inner
    and inner_executing_account = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
    and cardinality(account_arguments) = 1
    and substr(cast(data as varchar),3,16)='e445a52e51cb9a1d'
    and substr(cast(data as varchar),19,16)='516ce3becdd00ac4'
    {% if is_incremental() %}
      and {{ incremental_predicate('block_time') }}
    {% else %}
      and block_time >= timestamp '{{ project_start_date }}'
    {% endif %}
),

final as (
  select
    'solana' as blockchain,
    'meteora' as project,
    2 as version,
    cast(date_trunc('month', sw.block_time) as date) as block_month,
    sw.block_time,
    sw.block_slot,
    case 
      when sw.is_inner_swap = false then 'direct'
      else sw.outer_executing_account 
    end as trade_source,
    bytearray_to_uint256(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),195,16)),1,16))) as token_bought_amount_raw,
    bytearray_to_uint256(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),179,16)),1,16))) as token_sold_amount_raw,
    cast(null as double) as fee_tier,
    case 
      when bytearray_to_uint256(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),211,2)),1,2))) = 1 
      then sw.account_tokenXMint 
      else sw.account_tokenYMint
    end as token_sold_mint_address,
    case 
      when bytearray_to_uint256(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),211,2)),1,2))) = 0 
      then sw.account_tokenXMint 
      else sw.account_tokenYMint
    end as token_bought_mint_address,
    case 
      when bytearray_to_uint256(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),211,2)),1,2))) = 1 
      then sw.account_reserveX 
      else sw.account_reserveY
    end as token_sold_vault,
    case 
      when bytearray_to_uint256(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),211,2)),1,2))) = 0 
      then sw.account_reserveX 
      else sw.account_reserveY
    end as token_bought_vault,
    sw.account_lbPair as project_program_id,
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo' as project_main_id,
    sw.trader_id,
    sw.tx_id,
    sw.outer_instruction_index,
    sw.inner_instruction_index,
    sw.tx_index
  from all_swaps sw
  left join inner_instruct ic
    on sw.tx_id = ic.tx_id
   and sw.block_time = ic.block_time
   and sw.outer_instruction_index = ic.outer_instruction_index
   and sw.swap_number = ic.swap_number
)

select * from final
