{{
  config(
    schema = 'aquifer_solana'
    , alias = 'v2_stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2026-01-30' %}

-- Aquifer v2 swaps from instruction_calls (discriminator 0x13)
WITH swaps AS (
  SELECT
    block_slot
    , cast(date_trunc('month', block_date) AS DATE) AS block_month
    , block_date
    , block_time
    , COALESCE(inner_instruction_index, 0) AS inner_instruction_index
    , outer_instruction_index
    , outer_executing_account
    , is_inner
    , tx_id
    , tx_signer
    , tx_index
    , CAST(NULL AS VARCHAR) AS pool_id
    , {{ solana_instruction_key(
          'block_slot'
        , 'tx_index'
        , 'outer_instruction_index'
        , 'inner_instruction_index'
      ) }} AS surrogate_key
  FROM {{ source('solana', 'instruction_calls') }}
  WHERE
    1=1
    AND executing_account = 'AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45'
    AND BYTEARRAY_SUBSTRING(data, 1, 1) = 0x13
    AND tx_success = true
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_date') }}
    {% else -%}
    AND block_date >= DATE '{{ project_start_date }}'
    {% endif -%}
)
SELECT * FROM swaps
