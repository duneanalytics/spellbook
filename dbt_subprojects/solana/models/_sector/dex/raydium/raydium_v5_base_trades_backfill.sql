{{
  config(
    schema = 'raydium_v5'
    , alias = 'base_trades_backfill'
    , tags = ['microbatch']
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'microbatch'
    , event_time = 'block_date'
    , begin = '2024-05-16'
    , batch_size = var('raydium_v5_batch_size', 'day')
    , lookback = 1
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

with swaps as (
    select
          block_slot
        , block_month
        , block_date
        , block_time
        , inner_instruction_index
        , outer_instruction_index
        , outer_executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , pool_id
        , surrogate_key
    from {{ ref('raydium_v5_solana_stg_decoded_swaps') }}
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
    from {{ source('tokens_solana', 'transfers') }}
    where
        token_version = 'spl_token' or token_version = 'spl_token_2022'
)
, all_swaps as (
    select
          sp.block_time
        , sp.block_month
        , sp.block_date
        , sp.block_slot
        , case when sp.is_inner = false then 'direct'
            else sp.outer_executing_account
            end as trade_source
        , trs_2.amount as token_bought_amount_raw
        , trs_1.amount as token_sold_amount_raw
        , sp.pool_id
        , sp.tx_signer as trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.inner_instruction_index
        , sp.tx_index
        , trs_2.token_mint_address as token_bought_mint_address
        , trs_1.token_mint_address as token_sold_mint_address
        , trs_2.from_token_account as token_bought_vault
        , trs_1.to_token_account as token_sold_vault
        , sp.surrogate_key
    from swaps as sp
    inner join transfers as trs_1
        on trs_1.tx_id = sp.tx_id
        and trs_1.outer_instruction_index = sp.outer_instruction_index
        and trs_1.inner_instruction_index = sp.inner_instruction_index + 1
    inner join transfers as trs_2
        on trs_2.tx_id = sp.tx_id
        and trs_2.outer_instruction_index = sp.outer_instruction_index
        and trs_2.inner_instruction_index = sp.inner_instruction_index + 2
)

select
      'solana' as blockchain
    , 'raydium' as project
    , 5 as version
    , 'cpmm' as version_name
    , tb.block_month
    , tb.block_date
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
    , 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , tb.surrogate_key
from all_swaps as tb
