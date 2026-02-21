{{
  config(
    schema = 'humidifi_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

WITH swaps AS (
    SELECT
          block_slot
        , CAST(date_trunc('month', block_date) AS DATE) AS block_month
        , block_date
        , block_time
        , COALESCE(inner_instruction_index, 0) AS inner_instruction_index
        , outer_instruction_index
        , outer_executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , account_arguments[2] AS pool_id
        , account_arguments[3] AS vault_a
        , account_arguments[4] AS vault_b
        , {{ solana_instruction_key(
              'block_slot'
            , 'tx_index'
            , 'outer_instruction_index'
            , 'inner_instruction_index'
          ) }} AS surrogate_key
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE 1=1
        AND executing_account = '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'
        AND tx_success = true
        AND cardinality(account_arguments) > 8
        {% if is_incremental() or true -%}
        AND {{ incremental_predicate('block_date') }}
        {% else -%}
        AND block_date >= DATE '2025-06-13'
        {% endif -%}
)

SELECT * FROM swaps
