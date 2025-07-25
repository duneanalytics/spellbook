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

swaps_call_data as (
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

evt_table as (
  select
    evt_tx_id as tx_id,
    evt_block_time as block_time,
    coalesce(evt_tx_index,0) as tx_index,
    coalesce(evt_outer_instruction_index,0) as outer_instruction_index,
    coalesce(evt_inner_instruction_index,0) as inner_instruction_index,
    row_number() over ( partition by evt_tx_id, evt_outer_instruction_index order by coalesce(evt_inner_instruction_index,0) asc ) as swap_number,
    amountIn,
    amountOut,
    swapForY
  from {{ source('dlmm_solana','lb_clmm_evt_swap') }}
  where 1=1
    and evt_inner_executing_account = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
    {% if is_incremental() %}
      and {{ incremental_predicate('evt_block_time') }}
    {% else %}
      and evt_block_time >= timestamp '{{ project_start_date }}'
    {% endif %}
),

met_v2_trades as (
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
    amountOut as token_bought_amount_raw,
    amountIn as token_sold_amount_raw,
    cast(null as double) as fee_tier,
    case 
      when swapForY = True
      then sw.account_tokenXMint 
      else sw.account_tokenYMint
    end as token_sold_mint_address,
    case 
      when swapForY = False
      then sw.account_tokenXMint 
      else sw.account_tokenYMint
    end as token_bought_mint_address,
    case 
      when swapForY = True
      then sw.account_reserveX 
      else sw.account_reserveY
    end as token_sold_vault,
    case 
      when swapForY = False 
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
  from swaps_call_data sw
  left join evt_table evt
    on sw.tx_id = evt.tx_id
   and sw.block_time = evt.block_time
   and sw.outer_instruction_index = evt.outer_instruction_index
   and sw.swap_number = evt.swap_number
)

select * from met_v2_trades


