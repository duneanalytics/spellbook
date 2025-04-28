{{
  config(
    schema = 'stabble_solana',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index', 'block_month']
  )
}}

{% set project_start_date = '2024-04-01' %}

WITH all_swaps AS (
    -- Stable Swap V1
    SELECT
        call_block_time AS block_time,
        call_block_slot AS block_slot,
        'stabble' AS project,
        1 AS version,
        'solana' AS blockchain,
        CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END AS trade_source,
        amount_in AS token_sold_amount_raw,
        minimum_amount_out AS token_bought_amount_raw,
        account_pool AS pool_id,
        call_tx_signer AS trader_id,
        call_tx_id AS tx_id,
        call_outer_instruction_index AS outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0) AS inner_instruction_index,
        call_tx_index AS tx_index,
        account_user_token_in AS token_sold_mint_address,
        account_user_token_out AS token_bought_mint_address,
        account_vault_token_in AS token_sold_vault,
        account_vault_token_out AS token_bought_vault
    FROM {{ source('stable_swap_solana', 'stable_swap_call_swap') }}
    WHERE call_block_time >= TIMESTAMP '{{ project_start_date }}'

    UNION ALL

    -- Stable Swap V2
    SELECT
        call_block_time,
        call_block_slot,
        'stabble',
        1,
        'solana',
        CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END,
        amount_in,
        minimum_amount_out,
        account_pool,
        call_tx_signer,
        call_tx_id,
        call_outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0),
        call_tx_index,
        account_user_token_in,
        account_user_token_out,
        account_vault_token_in,
        account_vault_token_out
    FROM {{ source('stable_swap_solana', 'stable_swap_call_swap_v2') }}
    WHERE call_block_time >= TIMESTAMP '{{ project_start_date }}'

    UNION ALL

    -- Weighted Swap V1
    SELECT
        call_block_time,
        call_block_slot,
        'stabble',
        1,
        'solana',
        CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END,
        amount_in,
        minimum_amount_out,
        account_pool,
        call_tx_signer,
        call_tx_id,
        call_outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0),
        call_tx_index,
        account_user_token_in,
        account_user_token_out,
        account_vault_token_in,
        account_vault_token_out
    FROM {{ source('stable_swap_solana', 'weighted_swap_call_swap') }}
    WHERE call_block_time >= TIMESTAMP '{{ project_start_date }}'

    UNION ALL

    -- Weighted Swap V2
    SELECT
        call_block_time,
        call_block_slot,
        'stabble',
        1,
        'solana',
        CASE WHEN call_is_inner = FALSE THEN 'direct' ELSE call_outer_executing_account END,
        amount_in,
        minimum_amount_out,
        account_pool,
        call_tx_signer,
        call_tx_id,
        call_outer_instruction_index,
        COALESCE(call_inner_instruction_index, 0),
        call_tx_index,
        account_user_token_in,
        account_user_token_out,
        account_vault_token_in,
        account_vault_token_out
    FROM {{ source('stable_swap_solana', 'weighted_swap_call_swap_v2') }}
    WHERE call_block_time >= TIMESTAMP '{{ project_start_date }}'
)

SELECT
    tb.blockchain,
    tb.project,
    tb.version,
    CAST(DATE_TRUNC('month', tb.block_time) AS DATE) AS block_month,
    tb.block_time,
    tb.block_slot,
    tb.trade_source,
    tb.token_bought_amount_raw,
    tb.token_sold_amount_raw,
    CAST(NULL AS DOUBLE) AS fee_tier,
    tb.token_sold_mint_address,
    tb.token_bought_mint_address,
    tb.token_sold_vault,
    tb.token_bought_vault,
    tb.pool_id AS project_program_id,
    'swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ' AS project_main_id,
    tb.trader_id,
    tb.tx_id,
    tb.outer_instruction_index,
    tb.inner_instruction_index,
    tb.tx_index
FROM all_swaps tb
