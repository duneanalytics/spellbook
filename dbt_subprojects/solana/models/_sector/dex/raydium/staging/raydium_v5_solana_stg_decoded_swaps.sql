{{
  config(
    schema = 'raydium_v5_solana'
    , alias = 'stg_decoded_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2024-05-16' %}

SELECT
      sp.call_block_slot AS block_slot
    , CAST(date_trunc('month', sp.call_block_time) AS DATE) AS block_month
    , sp.call_block_date AS block_date
    , sp.call_block_time AS block_time
    , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
    , sp.call_outer_instruction_index AS outer_instruction_index
    , sp.call_outer_executing_account AS outer_executing_account
    , sp.call_is_inner AS is_inner
    , sp.call_tx_id AS tx_id
    , sp.call_tx_signer AS tx_signer
    , sp.call_tx_index AS tx_index
    , sp.account_poolState AS pool_id
    , {{ solana_instruction_key(
          'sp.call_block_slot'
        , 'sp.call_tx_index'
        , 'sp.call_outer_instruction_index'
        , 'COALESCE(sp.call_inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM (
    SELECT
          account_poolState
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
    FROM {{ source('raydium_cp_solana', 'raydium_cp_swap_call_swapBaseOutput') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        AND call_block_date < DATE_ADD('day', 7, DATE '{{ project_start_date }}')
        {% endif %}

    UNION ALL

    SELECT
          account_poolState
        , call_is_inner
        , call_outer_instruction_index
        , call_inner_instruction_index
        , call_tx_id
        , call_block_time
        , call_block_slot
        , call_block_date
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
    FROM {{ source('raydium_cp_solana', 'raydium_cp_swap_call_swapBaseInput') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_date >= DATE '{{ project_start_date }}'
        AND call_block_date < DATE_ADD('day', 7, DATE '{{ project_start_date }}')
        {% endif %}
) sp
