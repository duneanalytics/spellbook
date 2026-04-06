{{
  config(
    schema = 'manifest_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

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
    , account_arguments[6] AS vault_a
    , account_arguments[7] AS vault_b
    , account_arguments[8] AS vault_c
    , BYTEARRAY_SUBSTRING(data, 1, 1) AS disc
    , {{ solana_instruction_key(
          'block_slot'
        , 'tx_index'
        , 'outer_instruction_index'
        , 'COALESCE(inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM {{ source('solana', 'instruction_calls') }}
WHERE 1=1
    AND executing_account = 'MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms'
    AND tx_success = true
    AND BYTEARRAY_SUBSTRING(data, 1, 1) IN (0x0d, 0x04)
    AND cardinality(account_arguments) > 8
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_date') }}
    {% else -%}
    AND block_date >= DATE '2026-04-01'
    {% endif -%}
