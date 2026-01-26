{{
  config(
    schema = 'alphaq_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2025-08-29' %}

-- alphaq swap data from instruction_calls table
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
    , CAST(NULL as VARCHAR) AS pool_id -- AlphaQ does not use a pool system like other AMM's. Each token has it's own single vault.
    , {{ solana_instruction_key(
          'block_slot'
        , 'tx_index'
        , 'outer_instruction_index'
        , 'inner_instruction_index'
      ) }} as surrogate_key
  FROM {{ source('solana','instruction_calls') }}
  WHERE
    1=1
    AND executing_account = 'ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA'
    AND tx_success = true
    AND BYTEARRAY_SUBSTRING(data, 1, 1) = 0x0c
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_date') }}
    {% else -%}
    AND block_date >= DATE '{{ project_start_date }}'
    {% endif -%}
)
select *
from swaps
