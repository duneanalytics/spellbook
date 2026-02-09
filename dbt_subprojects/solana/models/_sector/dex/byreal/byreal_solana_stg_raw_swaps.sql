{{
  config(
    schema = 'byreal_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2025-06-26' %}

-- byreal swap data from instruction_calls table
WITH swaps AS (
  SELECT
    block_slot
    , cast(date_trunc('month', block_date) AS DATE) AS block_month
    , block_date
    , block_time
    , COALESCE(inner_instruction_index,0) as inner_instruction_index -- adjust to index 0 for direct trades
    , outer_instruction_index
    , inner_executing_account
    , outer_executing_account
    , executing_account
    , is_inner
    , tx_id
    , tx_signer
    , tx_index
    , account_arguments[3] AS pool_id
    , {{ solana_instruction_key(
          'block_slot'
        , 'tx_index'
        , 'outer_instruction_index'
        , 'inner_instruction_index'
      ) }} as surrogate_key
  FROM {{ source('solana','instruction_calls') }}
  WHERE
    1=1
    AND executing_account = 'REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2' -- Byreal CLMM
    AND tx_success = true
    AND BYTEARRAY_SUBSTRING(data, 1, 8) in (0x2b04ed0b1ac91e62,0xf8c69e91e17587c8) -- swap_v2,swap
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_date') }}
    {% else -%}
    AND block_date >= DATE '{{ project_start_date }}'
    AND block_date < DATE '{{ project_start_date }}' + INTERVAL '1' DAY
    {% endif -%}
)
select *
from swaps
