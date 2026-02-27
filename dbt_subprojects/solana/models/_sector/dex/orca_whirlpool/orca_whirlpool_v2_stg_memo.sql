{{
  config(
    schema = 'orca_whirlpool_v2'
    , alias = 'stg_memo'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2024-06-05' %}

SELECT
      ic.block_slot
    , CAST(date_trunc('month', ic.block_time) AS DATE) AS block_month
    , CAST(date_trunc('day', ic.block_time) AS DATE) AS block_date
    , ic.block_time
    , ic.tx_id
    , ic.tx_index
    , ic.outer_instruction_index
    , ic.inner_instruction_index
    , {{ solana_instruction_key(
          'ic.block_slot'
        , 'ic.tx_index'
        , 'ic.outer_instruction_index'
        , 'ic.inner_instruction_index'
      ) }} AS surrogate_key
FROM {{ source('solana', 'instruction_calls') }} ic
WHERE 1=1
    AND ic.executing_account = 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr'
    {% if is_incremental() %}
    AND {{ incremental_predicate('ic.block_time') }}
    {% else %}
    AND ic.block_time >= TIMESTAMP '{{ project_start_date }}'
    AND ic.block_time < TIMESTAMP '2024-06-12'
    {% endif %}
