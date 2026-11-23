{{
 config(
       schema = 'raydium_v3',
       alias = 'base_trades_backfill',
       tags = ['microbatch'],
       partition_by = ['block_month'],
       materialized = 'incremental',
       file_format = 'delta',
       incremental_strategy = 'microbatch',
       event_time = 'block_time',
       begin = '2022-08-17',
       batch_size = var('raydium_v3_batch_size', 'day'),
       lookback = 1,
       unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
       pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
       )
}}

with swaps as (
    select
        account_poolState
        , account_inputVault
        , account_outputVault
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
    from {{ source('raydium_clmm_solana', 'amm_v3_call_swap') }}
    union all
    select
        account_poolState
        , account_inputVault
        , account_outputVault
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
    from {{ source('raydium_clmm_solana', 'amm_v3_call_swapV2') }}
)
, transfers as (
    select
        tx_id
        , outer_instruction_index
        , inner_instruction_index
        , amount
        , token_mint_address
        , from_token_account
        , to_token_account
    from {{ source('tokens_solana','transfers') }}
)
, all_swaps as (
    select
        sp.call_block_time as block_time
        , 'raydium' as project
        , 3 as version
        , 'solana' as blockchain
        , sp.call_block_slot as block_slot
        , case when sp.call_is_inner = false then 'direct'
            else sp.call_outer_executing_account
            end as trade_source
        , tr_2.amount as token_bought_amount_raw
        , tr_1.amount as token_sold_amount_raw
        , sp.account_poolState as pool_id
        , sp.call_tx_signer as trader_id
        , sp.call_tx_id as tx_id
        , sp.call_outer_instruction_index as outer_instruction_index
        , coalesce(sp.call_inner_instruction_index, 0) as inner_instruction_index
        , sp.call_tx_index as tx_index
        , tr_2.token_mint_address as token_bought_mint_address
        , tr_1.token_mint_address as token_sold_mint_address
        , sp.account_outputVault as token_bought_vault
        , sp.account_inputVault as token_sold_vault
    from swaps as sp
    inner join transfers as tr_1
        on tr_1.tx_id = sp.call_tx_id
        and tr_1.outer_instruction_index = sp.call_outer_instruction_index
        and ((sp.call_is_inner = false and tr_1.inner_instruction_index = 1)
            or (sp.call_is_inner = true and tr_1.inner_instruction_index = sp.call_inner_instruction_index + 1))
    inner join transfers as tr_2
        on tr_2.tx_id = sp.call_tx_id
        and tr_2.outer_instruction_index = sp.call_outer_instruction_index
        and ((sp.call_is_inner = false and tr_2.inner_instruction_index = 2)
            or (sp.call_is_inner = true and tr_2.inner_instruction_index = sp.call_inner_instruction_index + 2))
)

select
    tb.blockchain
    , tb.project
    , tb.version
    , cast(date_trunc('month', tb.block_time) as date) as block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , cast(null as double) as fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id as project_program_id
    , 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
from
    all_swaps as tb
