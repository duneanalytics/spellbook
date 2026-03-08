{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'stg_decoded_newevent'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2026-03-08' %}

    -- New buy event decoded from raw instruction_calls (discriminator: 0xe445a52e51cb9a1d67f4521f2cf57777)
    WITH new_buy_events_raw AS (
        SELECT
              g.block_time
            , g.block_slot
            , g.block_date
            , g.outer_instruction_index
            , f.inner_instruction_index
            , g.tx_id
            , g.tx_index
            , g.outer_executing_account
            , to_base58(bytearray_substring(g.data, 129, 32)) AS account_pool
            , to_base58(bytearray_substring(g.data, 161, 32)) AS account_user
            , to_base58(bytearray_substring(g.data, 193, 32)) AS account_user_base_token_account
            , to_base58(bytearray_substring(g.data, 225, 32)) AS account_user_quote_token_account
            , f.account_arguments[8] AS account_pool_base_token_account  
            , f.account_arguments[9] AS account_pool_quote_token_account  
            , to_base58(bytearray_substring(g.data, 289, 32)) AS account_protocol_fee_recipient_token_account
            , bytearray_to_uint256(bytearray_reverse(bytearray_substring(g.data, 25, 8))) AS base_amount
            , ROW_NUMBER() OVER (
                PARTITION BY g.tx_id, g.outer_instruction_index, g.inner_instruction_index 
                ORDER BY f.inner_instruction_index
              ) AS rn    -- To avoid potential duplicate rows rom the join
        FROM {{ source('solana', 'instruction_calls') }} g
        JOIN {{ source('solana', 'instruction_calls') }} f 
            ON g.tx_id = f.tx_id
            AND g.block_date = f.block_date
            AND g.outer_instruction_index = f.outer_instruction_index
            AND bytearray_substring(f.data, 1, 8) = 0xc62e1552b4d9e870
            AND f.executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
            AND f.tx_success = true
            AND f.block_time >= TIMESTAMP '{{ project_start_date }}'
            {% if is_incremental() %}
            AND {{ incremental_predicate('f.block_time') }}
            {% endif %}
        WHERE bytearray_substring(g.data, 1, 16) = 0xe445a52e51cb9a1d67f4521f2cf57777
            AND g.executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
            AND g.is_inner = true
            AND g.tx_success = true
            AND length(g.data) = 447
            AND g.block_time >= TIMESTAMP '{{ project_start_date }}'
            {% if is_incremental() %}
            AND {{ incremental_predicate('g.block_time') }}
            {% endif %}
    )

    SELECT
          block_slot
        , CAST(date_trunc('month', block_date) AS DATE) AS block_month
        , block_date
        , block_time
        , COALESCE(inner_instruction_index, 0) AS inner_instruction_index
        , inner_instruction_index AS swap_inner_index
        , outer_instruction_index
        , outer_executing_account
        , tx_id
        , tx_index
        , account_pool as pool
        , account_user as user_account
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient_token_account
        , base_amount
        , 1 AS is_buy
        , {{ solana_instruction_key(
          'block_slot'
        , 'tx_index'
        , 'outer_instruction_index'
        , 'COALESCE(inner_instruction_index, 0)'
      ) }} AS surrogate_key
    FROM new_buy_events_raw
    WHERE rn = 1