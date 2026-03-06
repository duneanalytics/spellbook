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

{% set project_start_date = '2025-02-20' %}

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
        , 1 AS is_buy
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_date') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
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
        , 0 AS is_buy
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_sell') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_date') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

SELECT
      sp.call_block_slot AS block_slot
    , CAST(date_trunc('month', sp.call_block_date) AS DATE) AS block_month
    , sp.call_block_date AS block_date
    , sp.call_block_time AS block_time
    , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
    , sp.call_inner_instruction_index AS swap_inner_index
    , sp.call_outer_instruction_index AS outer_instruction_index
    , sp.call_outer_executing_account AS outer_executing_account
    , sp.call_tx_id AS tx_id
    , sp.call_tx_index AS tx_index
    , sp.account_pool AS pool
    , sp.account_user AS user_account
    , sp.account_user_base_token_account
    , sp.account_user_quote_token_account
    , sp.account_pool_base_token_account
    , sp.account_pool_quote_token_account
    , sp.account_protocol_fee_recipient_token_account
    , sp.base_amount
    , sp.is_buy
    , {{ solana_instruction_key(
          'sp.call_block_slot'
        , 'sp.call_tx_index'
        , 'sp.call_outer_instruction_index'
        , 'COALESCE(sp.call_inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM swaps sp
