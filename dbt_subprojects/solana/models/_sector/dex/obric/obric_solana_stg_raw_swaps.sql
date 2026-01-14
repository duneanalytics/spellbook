{{
  config(
    schema = 'obric_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2026-01-01' %}

-- obric swap data from instruction_calls table. Filtered by program id & swap discriminator.
WITH swaps AS (
  SELECT
    block_slot
    , cast(date_trunc('month', block_date) AS DATE) AS block_month
    , block_date
    , block_time
    , COALESCE(inner_instruction_index,0) as inner_instruction_index -- adjust to index 0 for direct trades
    , outer_instruction_index
    , outer_executing_account
    , is_inner
    , tx_id
    , tx_signer
    , tx_index
    , account_arguments[1] AS pool_id
    , {{ solana_instruction_key(
          'block_slot'
        , 'tx_index'
        , 'outer_instruction_index'
        , 'inner_instruction_index'
      ) }} as surrogate_key
  FROM {{ source('solana','instruction_calls') }}
  WHERE
    1=1
    AND executing_account = 'obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y'
    AND BYTEARRAY_SUBSTRING(data, 1, 8) IN (0xf8c69e91e17587c8,0x414b3f4ceb5b5b88) -- Swap discriminator, 8 bytes. See: https://dune.com/queries/5857407
    AND tx_success = true
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_date') }}
    {% else -%}
    AND block_date >= DATE '{{ project_start_date }}'
    {% endif -%}
)
select *
from swaps
