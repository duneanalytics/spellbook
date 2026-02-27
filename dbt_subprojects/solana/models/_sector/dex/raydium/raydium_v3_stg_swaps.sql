{{
  config(
    schema = 'raydium_v3'
    , alias = 'stg_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2022-08-17' %}

WITH pools AS (
    SELECT
          ip.account_tokenMint0 AS tokenA
        , ip.account_tokenVault0 AS tokenAVault
        , ip.account_tokenMint1 AS tokenB
        , ip.account_tokenVault1 AS tokenBVault
        , ip.account_ammConfig AS fee_tier
        , ip.account_poolState AS pool_id
        , row_number() OVER (PARTITION BY ip.account_poolState ORDER BY ip.call_block_time DESC) AS recent_init
    FROM {{ source('raydium_clmm_solana', 'amm_v3_call_createPool') }} ip
)

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
    , p.pool_id
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
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
    FROM {{ source('raydium_clmm_solana', 'amm_v3_call_swap') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND call_block_time < TIMESTAMP '2022-08-24'
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
        , call_outer_executing_account
        , call_tx_signer
        , call_tx_index
    FROM {{ source('raydium_clmm_solana', 'amm_v3_call_swapV2') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('call_block_time') }}
        {% else %}
        AND call_block_time >= TIMESTAMP '{{ project_start_date }}'
        AND call_block_time < TIMESTAMP '2022-08-24'
        {% endif %}
) sp
INNER JOIN pools p
    ON sp.account_poolState = p.pool_id
    AND p.recent_init = 1
