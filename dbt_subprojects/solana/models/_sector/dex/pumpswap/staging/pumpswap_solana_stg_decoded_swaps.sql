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
        , 0 AS is_buy
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_sell') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}

    UNION ALL

  
  -- New buy event decoded from raw instruction_calls (discriminator: 0xe445a52e51cb9a1d67f4521f2cf57777) 
 
 SELECT
          g.block_time AS call_block_time
        , g.block_slot AS call_block_slot
        , CAST(g.block_date AS DATE) AS call_block_date
        , g.outer_instruction_index AS call_outer_instruction_index
        , g.inner_instruction_index AS call_inner_instruction_index
        , g.tx_id AS call_tx_id
        , g.tx_index AS call_tx_index
        , g.outer_executing_account AS call_outer_executing_account
        , to_base58(bytearray_substring(g.data, 129, 32)) AS account_pool
        , to_base58(bytearray_substring(g.data, 161, 32)) AS account_user
        , to_base58(bytearray_substring(g.data, 193, 32)) AS account_user_base_token_account
        , to_base58(bytearray_substring(g.data, 225, 32)) AS account_user_quote_token_account
        , f.account_arguments[8] AS account_pool_base_token_account  
        , f.account_arguments[9] AS account_pool_quote_token_account  
        , to_base58(bytearray_substring(g.data, 289, 32)) AS account_protocol_fee_recipient_token_account
        , bytearray_to_uint256(bytearray_reverse(bytearray_substring(g.data, 25, 8))) AS base_amount
        , 1 AS is_buy
    FROM {{ source('solana', 'instruction_calls') }} g
    JOIN {{ source('solana', 'instruction_calls') }} f 
        ON g.tx_id = f.tx_id
        AND bytearray_substring(f.data, 1, 8) = 0xc62e1552b4d9e870
        AND f.executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
        AND f.tx_success = true
        {% if is_incremental() %}
        AND {{ incremental_predicate('f.block_time') }}
        {% else %}
        AND f.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
    WHERE bytearray_substring(g.data, 1, 16) = 0xe445a52e51cb9a1d67f4521f2cf57777
        AND g.executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
        AND g.is_inner = true
        AND g.tx_success = true
        AND length(g.data) = 447
        {% if is_incremental() %}
        AND {{ incremental_predicate('g.block_time') }}
        {% else %}
        AND g.block_time >= TIMESTAMP '{{ project_start_date }}'
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