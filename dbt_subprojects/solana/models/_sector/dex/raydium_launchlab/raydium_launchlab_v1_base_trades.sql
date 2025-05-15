{{
  config(
        schema = 'raydium_launchlab_v1',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'block_slot', 'tx_index', 'outer_instruction_index', 'inner_instruction_index', 'token_bought_vault', 'token_sold_vault'],
        pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
        )
}}
{% set project_start_date = '2025-03-17' %}
with calls as (
    select
        account_pool_state
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
        , account_base_token_mint
        , account_quote_token_mint
        ,account_platform_config
        ,account_base_vault
        ,account_quote_vault
    from
        {{ source('raydium_solana', 'raydium_launchpad_call_buy_exact_in') }}
    {% if is_incremental() -%}
        where
            {{incremental_predicate('call_block_time')}}
    {% else -%}
        where
            call_block_time >= TIMESTAMP '{{project_start_date}}'

    {% endif -%}
    union all
    select
        account_pool_state
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
        , account_base_token_mint
        , account_quote_token_mint
        , account_platform_config
        ,account_base_vault
        ,account_quote_vault
    from
        {{ source('raydium_solana', 'raydium_launchpad_call_buy_exact_out') }}
    {% if is_incremental() -%}
        where
            {{incremental_predicate('call_block_time')}}
    {% else -%}
        where
            call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif -%}
    union all
    select
        account_pool_state
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
        , account_base_token_mint
        , account_quote_token_mint
        , account_platform_config
        ,account_base_vault
        ,account_quote_vault
    from
        {{ source('raydium_solana', 'raydium_launchpad_call_sell_exact_in') }}
    {% if is_incremental() -%}
        where
            {{incremental_predicate('call_block_time')}}
    {% else -%}
        where
            call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif -%}
    union all
    select
        account_pool_state
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
        , account_base_token_mint
        , account_quote_token_mint
        , account_platform_config
        ,account_base_vault
        ,account_quote_vault
    from
        {{ source('raydium_solana', 'raydium_launchpad_call_sell_exact_out') }}
    {% if is_incremental() -%}
        where
            {{incremental_predicate('call_block_time')}}
    {% else -%}
        where
            call_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif -%}
)

, all_swaps as (
    SELECT
        sp.call_block_time as block_time
        , sp.call_block_slot as block_slot
        , 'raydium_launchlab' as project
        , 1 as version
        , 'solana' as blockchain
        , case when sp.call_is_inner = False then 'direct'
            else sp.call_outer_executing_account
            end as trade_source
        , sp.account_platform_config as account_platform_config
        -- -- token bought is always the second instruction (transfer) in the inner instructions
        , trs_base.amount as token_bought_amount_raw
        , trs_quote.amount as token_sold_amount_raw
        , account_pool_state as pool_id
        , sp.call_tx_signer as trader_id
        , sp.call_tx_id as tx_id
        , sp.call_outer_instruction_index as outer_instruction_index
        , COALESCE(sp.call_inner_instruction_index, 0) as inner_instruction_index
        , sp.call_tx_index as tx_index
        , COALESCE(trs_base.token_mint_address, cast(null as varchar)) as token_bought_mint_address
        , COALESCE(trs_quote.token_mint_address, cast(null as varchar)) as token_sold_mint_address
        , trs_base.from_token_account as token_bought_vault
        , trs_quote.to_token_account as token_sold_vault
    FROM calls as sp
    INNER JOIN {{ ref('tokens_solana_transfers') }} as trs_base
        ON trs_base.tx_id = sp.call_tx_id 
        AND trs_base.block_slot = sp.call_block_slot
        AND trs_base.outer_instruction_index = sp.call_outer_instruction_index
        AND trs_base.token_mint_address = sp.account_base_token_mint
        AND trs_base.to_token_account = sp.account_base_vault
        AND (
            (sp.call_is_inner = false AND trs_base.inner_instruction_index BETWEEN 1 AND 24)
            OR (sp.call_is_inner = true AND trs_base.inner_instruction_index BETWEEN sp.call_inner_instruction_index + 1 AND sp.call_inner_instruction_index + 24)
        )
        AND trs_base.block_time >= TIMESTAMP '{{project_start_date}}'
    INNER JOIN {{ ref('tokens_solana_transfers') }} as trs_quote
        ON trs_quote.tx_id = sp.call_tx_id 
        AND trs_quote.block_slot = sp.call_block_slot
        AND trs_quote.outer_instruction_index = sp.call_outer_instruction_index
        AND trs_quote.token_mint_address = sp.account_quote_token_mint
        AND trs_quote.to_token_account = sp.account_quote_vault
        AND (
            (sp.call_is_inner = false AND trs_quote.inner_instruction_index BETWEEN 1 AND 24)
            OR (sp.call_is_inner = true AND trs_quote.inner_instruction_index BETWEEN sp.call_inner_instruction_index + 1 AND sp.call_inner_instruction_index + 24)
        )
        AND trs_quote.block_time >= TIMESTAMP '{{project_start_date}}'
)

SELECT
    tb.blockchain
    , tb.project
    , tb.version
    , CAST(date_trunc('month', tb.block_time) AS DATE) as block_month
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
    , 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj' as project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , tb.account_platform_config
FROM all_swaps tb

