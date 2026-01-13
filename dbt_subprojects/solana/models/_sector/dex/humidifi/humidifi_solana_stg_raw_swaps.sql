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

{% set project_start_date = '2026-01-01' %}

-- humidifi swap data from instruction_calls table
WITH swaps AS (
  SELECT
      block_slot
    , block_date
    , block_time
    , COALESCE(inner_instruction_index,0) as inner_instruction_index -- adjust to index 0 for direct trades
    , outer_instruction_index
    , outer_executing_account
    , is_inner
    , tx_id
    , tx_signer
    , tx_index
    , account_arguments[2] AS pool_id
    , {{ dbt_utils.generate_surrogate_key(['tx_id', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']) }} as surrogate_key
  FROM {{ source('solana','instruction_calls') }}
  WHERE
    1=1
    AND executing_account = '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'
    AND tx_success = true
    --AND BYTEARRAY_SUBSTRING(data, 1, 1) = '0xXX' 
        -- No distinct SWAP discriminator, unreliable method of isolate Humidifi swap instructions. See: https://dune.com/queries/5857394 
        -- Alternative method: arguments = 9 for swaps, join on inner_insturction_index +1 & +2 on token transfers.
    AND cardinality(account_arguments) = 9 -- 9 arguments for all swap instructions. 3 arguments for all quote update instructions. No change in this pattern since deployment
    {% if is_incremental() -%}
    AND {{ incremental_predicate('block_date') }}
    {% else -%}
    AND block_date >= DATE '{{ project_start_date }}'
    {% endif -%}
)
select *
from swaps