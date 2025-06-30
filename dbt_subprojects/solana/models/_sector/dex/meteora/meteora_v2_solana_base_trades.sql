 {{
  config(
        schema = 'meteora_v2_solana',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month']
        )
}}

{% set project_start_date = '2023-11-01' %} --grabbed program deployed at time (account created at).

{% set project_start_date = '2025-06-29' %} 

with swaps_events_data as
(
select
    call_block_slot as block_slot
    , call_block_time as block_time
    , coalesce(call_tx_index,0) as tx_index
    , coalesce(call_outer_instruction_index,0) as outer_instruction_index
    , coalesce(call_inner_instruction_index,0) as inner_instruction_index
    , call_tx_id as tx_id
    , call_tx_signer as trader_id
    , call_outer_executing_account as outer_executing_account
    , account_tokenXMint
    , account_tokenYMint
    , account_reserveX
    , account_reserveY
    , account_lbPair
    , call_is_inner as is_inner_swap
    , row_number() over (partition by call_tx_id order by call_tx_index asc, call_outer_instruction_index asc, call_inner_instruction_index) as swap_number
from {{ source ('dlmm_solana','lb_clmm_call_swap') }}
where 1=1
-- and call_block_time >= timestamp '{{project_start_date}}'
            {% if is_incremental() %}
            AND {{incremental_predicate('call_block_time')}}
            {% else %}
            AND call_block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
),
inner_instruct_data as 
(
select
    tx_id
    , block_time
    , coalesce(tx_index,0) as tx_index
    , coalesce(outer_instruction_index,0) as outer_instruction_index
    , coalesce(inner_instruction_index,0) as  inner_instruction_index
    , row_number() over (partition by tx_id order by tx_index asc, outer_instruction_index asc, inner_instruction_index) as swap_number
    , data
from {{ source ('solana','instruction_calls') }}
where 1=1
{% if is_incremental() %}
            AND {{incremental_predicate('block_time')}}
            {% else %}
            AND block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
and tx_success
and is_inner
and inner_executing_account='LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo' 
and cardinality(account_arguments)=1
)
select 
    'solana' as blockchain
    , 'meteora' as project
    , '2' as version
    , DATE_TRUNC('month', sw.block_time) AS block_month
    , sw.block_time
    , sw.block_slot
    , CASE WHEN is_inner_swap = FALSE THEN 'direct' ELSE sw.outer_executing_account END AS trade_source
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),211,2)), 1, 2)))  as swapforY
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as VARCHAR),195,16)),1,16))) as token_bought_amount_raw
    , bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as VARCHAR),179,16)),1,16))) as token_sold_amount_raw
    , NULL AS fee_tier
    , case when bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),211,2)), 1, 2))) =1 
      then account_tokenXMint
      else account_tokenYMint
      end as token_sold_mint_address
    , case when bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(data as varchar),211,2)), 1, 2))) =0 
      then account_tokenXMint
      else account_tokenYMint
      end as token_bought_mint_address
    , case when bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(ic.data as varchar),211,2)), 1, 2))) =1 
      then account_reserveX
      else account_reserveY
      end AS token_sold_vault
    , case when bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(data as varchar),211,2)), 1, 2))) =0 
      then account_reserveX
      else account_reserveY
      end as token_bought_vault
    , sw.account_lbPair AS project_program_id
    , 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo' AS project_main_id
    , sw.trader_id
    , sw.tx_id
    , sw.outer_instruction_index
    , sw.inner_instruction_index
    , sw.tx_index
  
from 
swaps_events_data sw 
left join inner_instruct_data ic 
on (sw.tx_id=ic.tx_id and sw.block_time=ic.block_time and sw.swap_number=ic.swap_number)
where 1=1 
and sw.tx_id='3kDZJQo2fyyE6kiViKtmGbxCcTh7EDjADux9xkah5ytNhsSBbHKx4h1QcJNWTT5BBD4nyxqdN7hahzW4pE4AYTZT'
{% if is_incremental() %}
            AND {{incremental_predicate('sw.block_time')}}
            {% else %}
            AND sw.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
