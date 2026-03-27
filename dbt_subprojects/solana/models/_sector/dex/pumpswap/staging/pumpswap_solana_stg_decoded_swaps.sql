{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'stg_decoded_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2026-03-06' %}

WITH swaps AS (
    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_tx_index
        , call_outer_executing_account
        , account_pool
        , account_user
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient_token_account
        , base_amount_out AS base_amount
        , CAST(NULL AS BIGINT) AS quote_amount
        , 1 AS is_buy
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
          call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_tx_index
        , call_outer_executing_account
        , account_pool
        , account_user
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient_token_account
        , base_amount_in AS base_amount
        , CAST(NULL AS BIGINT) AS quote_amount
        , 0 AS is_buy
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_sell') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
)

SELECT
      call_block_slot AS block_slot
    , CAST(date_trunc('month', call_block_date) AS DATE) AS block_month
    , call_block_date AS block_date
    , call_block_time AS block_time
    , COALESCE(call_inner_instruction_index, 0) AS inner_instruction_index
    , call_inner_instruction_index AS swap_inner_index
    , call_outer_instruction_index AS outer_instruction_index
    , call_outer_executing_account AS outer_executing_account
    , call_tx_id AS tx_id
    , call_tx_index AS tx_index
    , account_pool AS pool
    , account_user AS user_account
    , account_user_base_token_account
    , account_user_quote_token_account
    , account_pool_base_token_account
    , account_pool_quote_token_account
    , account_protocol_fee_recipient_token_account
    , base_amount
    , quote_amount
    , is_buy
    , {{ solana_instruction_key(
          'call_block_slot'
        , 'call_tx_index'
        , 'call_outer_instruction_index'
        , 'COALESCE(call_inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM swaps