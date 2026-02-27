{{
  config(
    schema = 'lifinity_v1'
    , alias = 'stg_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2022-01-26' %}

SELECT
      sp.call_block_slot AS block_slot
    , CAST(date_trunc('month', sp.call_block_time) AS DATE) AS block_month
    , CAST(date_trunc('day', sp.call_block_time) AS DATE) AS block_date
    , sp.call_block_time AS block_time
    , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
    , sp.call_outer_instruction_index AS outer_instruction_index
    , sp.call_outer_executing_account AS outer_executing_account
    , sp.call_is_inner AS is_inner
    , sp.call_tx_id AS tx_id
    , sp.call_tx_signer AS tx_signer
    , sp.call_tx_index AS tx_index
    , sp.account_amm AS pool_id
    , {{ solana_instruction_key(
          'sp.call_block_slot'
        , 'sp.call_tx_index'
        , 'sp.call_outer_instruction_index'
        , 'COALESCE(sp.call_inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM {{ source('lifinity_amm_solana', 'lifinity_amm_call_swap') }} sp
WHERE 1=1
    {% if is_incremental() %}
    AND {{ incremental_predicate('sp.call_block_time') }}
    {% else %}
    AND sp.call_block_time >= TIMESTAMP '{{ project_start_date }}'
    AND sp.call_block_time < TIMESTAMP '2022-02-02'
    {% endif %}
