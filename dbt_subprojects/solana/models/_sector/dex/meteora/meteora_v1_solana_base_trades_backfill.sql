{{
 config(
       schema = 'meteora_v1_solana',
       alias = 'base_trades_backfill',
       tags = ['microbatch'],
       partition_by = ['block_month'],
       materialized = 'incremental',
       file_format = 'delta',
       incremental_strategy = 'microbatch',
       event_time = 'block_time',
       begin = '2021-03-21',
       batch_size = var('meteora_v1_batch_size', 'day'),
       lookback = 3,
       unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index','block_month'],
       pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
       )
}}

{% set begin = '2021-03-21' %}
{% set batch_start = model.batch.event_time_start if model.batch else begin %}
{% set batch_end = model.batch.event_time_end if model.batch else '2099-01-01' %}

with swaps as (
    select
        sp.*
        , dp.call_inner_instruction_index as deposit_index
        , row_number() over (
            partition by sp.call_tx_id, sp.call_outer_instruction_index, sp.call_inner_instruction_index
            order by dp.call_inner_instruction_index asc
        ) as first_deposit
    from
        {{ source('meteora_pools_solana', 'amm_call_swap') }} sp
    left join
        {{ source('meteora_vault_solana', 'vault_call_deposit') }} dp
        on sp.call_tx_id = dp.call_tx_id
        and sp.call_block_slot = dp.call_block_slot
        and sp.call_outer_instruction_index = dp.call_outer_instruction_index
        and coalesce(sp.call_inner_instruction_index, 0) < dp.call_inner_instruction_index
        and dp.call_block_time >= timestamp '{{batch_start}}'
        and dp.call_block_time < timestamp '{{batch_end}}'
    where
        sp.call_block_time >= timestamp '{{batch_start}}'
        and sp.call_block_time < timestamp '{{batch_end}}'
)
, transfers as (
    select
        tx_id
        , block_date
        , block_time
        , outer_instruction_index
        , inner_instruction_index
        , amount
        , token_mint_address
        , from_token_account
        , to_token_account
    from
        {{ source('tokens_solana', 'transfers') }}
    where
        block_time >= timestamp '{{batch_start}}'
        and block_time < timestamp '{{batch_end}}'
)
, all_swaps as (
    select
        sp.call_block_time as block_time
        , sp.call_block_slot as block_slot
        , 'meteora' as project
        , 1 as version
        , 'solana' as blockchain
        , case when sp.call_is_inner = false then 'direct'
            else sp.call_outer_executing_account
            end as trade_source
        , trs_2.amount as token_bought_amount_raw
        , trs_1.amount as token_sold_amount_raw
        , sp.account_pool as pool_id
        , sp.call_tx_signer as trader_id
        , sp.call_tx_id as tx_id
        , sp.call_outer_instruction_index as outer_instruction_index
        , coalesce(sp.call_inner_instruction_index, 0) as inner_instruction_index
        , sp.call_tx_index as tx_index
        , coalesce(trs_2.token_mint_address, cast(null as varchar)) as token_bought_mint_address
        , coalesce(trs_1.token_mint_address, cast(null as varchar)) as token_sold_mint_address
        , trs_2.from_token_account as token_bought_vault
        , trs_1.to_token_account as token_sold_vault
    from
        swaps as sp
    inner join transfers as trs_1
        on trs_1.tx_id = sp.call_tx_id
        and trs_1.block_date = sp.call_block_date
        and trs_1.block_time = sp.call_block_time
        and trs_1.outer_instruction_index = sp.call_outer_instruction_index
        and trs_1.inner_instruction_index = sp.deposit_index + 1
    inner join transfers as trs_2
        on trs_2.tx_id = sp.call_tx_id
        and trs_2.block_date = sp.call_block_date
        and trs_2.block_time = sp.call_block_time
        and trs_2.outer_instruction_index = sp.call_outer_instruction_index
        and trs_2.inner_instruction_index = sp.deposit_index + 4
    where
        sp.first_deposit = 1
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
    , 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
from
    all_swaps as tb
