{{
  config(
    schema = 'meteora_v1_solana'
    , alias = 'stg_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2022-07-27' %}

WITH swaps AS (
    SELECT
          sp.call_block_slot
        , sp.call_block_date
        , sp.call_block_time
        , sp.call_is_inner
        , sp.call_outer_executing_account
        , sp.call_outer_instruction_index
        , sp.call_inner_instruction_index
        , sp.call_tx_id
        , sp.call_tx_signer
        , sp.call_tx_index
        , sp.account_pool
        , dp.call_inner_instruction_index AS deposit_index
        , ROW_NUMBER() OVER (
            PARTITION BY sp.call_tx_id, sp.call_outer_instruction_index, sp.call_inner_instruction_index
            ORDER BY dp.call_inner_instruction_index ASC
        ) AS first_deposit
    FROM {{ source('meteora_pools_solana', 'amm_call_swap') }} sp
    LEFT JOIN {{ source('meteora_vault_solana', 'vault_call_deposit') }} dp
        ON sp.call_tx_id = dp.call_tx_id
        AND sp.call_block_slot = dp.call_block_slot
        AND sp.call_outer_instruction_index = dp.call_outer_instruction_index
        AND COALESCE(sp.call_inner_instruction_index, 0) < dp.call_inner_instruction_index
        {% if is_incremental() %}
        AND {{ incremental_predicate('dp.call_block_time') }}
        {% else %}
        AND dp.call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('sp.call_block_time') }}
        {% else %}
        AND sp.call_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

SELECT
      sp.call_block_slot AS block_slot
    , CAST(date_trunc('month', sp.call_block_date) AS DATE) AS block_month
    , sp.call_block_date AS block_date
    , sp.call_block_time AS block_time
    , COALESCE(sp.call_inner_instruction_index, 0) AS inner_instruction_index
    , sp.call_outer_instruction_index AS outer_instruction_index
    , sp.call_outer_executing_account AS outer_executing_account
    , sp.call_is_inner AS is_inner
    , sp.call_tx_id AS tx_id
    , sp.call_tx_signer AS tx_signer
    , sp.call_tx_index AS tx_index
    , sp.account_pool AS pool_id
    , sp.deposit_index
    , {{ solana_instruction_key(
          'sp.call_block_slot'
        , 'sp.call_tx_index'
        , 'sp.call_outer_instruction_index'
        , 'COALESCE(sp.call_inner_instruction_index, 0)'
      ) }} AS surrogate_key
FROM swaps sp
WHERE sp.first_deposit = 1
