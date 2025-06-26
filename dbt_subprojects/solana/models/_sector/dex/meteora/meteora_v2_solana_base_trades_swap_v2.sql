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

{% set project_start_date = '2025-03-28 03:41' %} --grabbed program deployed at time (account created at).
{% set program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo' %} 
{% set account_arguments = 'D1ZN9Wj1fRSUQfCjhvnu1hqDMT7hzjzBBpi12nVniYD6' %} 

/* check this later 2GVhgeAKq2ALV19KrBymdp3Sdt4JQLJtPW12sdb9tkyfaPKfvQyNLEWNFAAZFPT198uZpjmrLZCZsxFbsWUxHCLb */
-- missing in spellbook approach 3KF2azAxy2nZxcKmq9yFBhtma6Z2ezzAPnXLJFuCA2Nx4U5ucdNUSXHQD4eSTSvBar6u2DcYug9bcAjky4zvodck
with swaps as 
(
    select sw.*, ic.data
    from {{ source('dlmm_solana','lb_clmm_call_swap2') }} sw
    left join {{ ref('solana','instruction_calls') }} ic 
    on 
        ( sw.call_tx_id=ic.tx_id and sw.call_block_time=ic.block_time and sw.call_tx_index=ic.tx_index and coalesce(sw.call_inner_instruction_index,0)+1 = ic.inner_instruction_index)
    where sw.call_block_time >= TIMESTAMP '{{project_start_date}}'
    and ic.block_time >= TIMESTAMP '{{project_start_date}}'
    -- and sw.call_tx_id='3X5xF4Byfdw4PM7c5ZUavniFABCFhZiYzadx4RDw6UhwQgcFRhabaCvVJW6cydwR1uXQvHn2dfFtR6uCY1PonhFM'
    and ic.tx_success
    and ic.is_inner
    and ic.inner_executing_account={{program_id}} 
    and account_arguments[1]=cast({{account_arguments}} as varchar)
),
swaps_data as 
(
    SELECT
    'solana' AS blockchain,
    'meteora' AS project,
    2 AS version,
    DATE_TRUNC('month', call_block_date) AS block_month,
    call_block_time AS block_time,
    call_block_slot AS block_slot,
    CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END AS trade_source,
    bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(data as varchar),211,2)), 1, 2)))  as swapforY,
    bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(data as VARCHAR),195,16)),1,16))) as token_bought_amount_raw,
    bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(data as VARCHAR),179,16)),1,16))) as token_sold_amount_raw ,
    NULL AS fee_tier,
    case when bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(data as varchar),211,2)), 1, 2))) =1
        then account_tokenXMint
        else account_tokenYMint
    end as token_sol_mint_address,
        case when bytearray_to_bigint(bytearray_reverse(bytearray_substring(('0x'||substr(cast(data as varchar),211,2)), 1, 2))) =0
        then account_tokenXMint
        else account_tokenYMint
    end as token_bought_mint_address,
    NULL AS token_sold_vault,
    NULL AS token_bought_vault,
    account_lbPair AS project_program_id,
    {{program_id}} AS project_main_id,
    call_tx_signer AS trader_id,
    call_tx_id as tx_id,
    call_outer_instruction_index,
    call_inner_instruction_index,
    call_tx_index
    FROM swaps
)
select *
from swaps_data     
