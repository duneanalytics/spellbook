{{
  config(
    schema = 'goonfi_v2_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'surrogate_key']
  )
}}

{% set project_start_date = '2025-12-12' %}

-- goonfi v2 swap data from staging table
WITH swaps AS (
    SELECT
          block_slot
        , block_date
        , block_time
        , inner_instruction_index
        , outer_instruction_index
        , outer_executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , pool_id
    FROM {{ ref('goonfi_v2_solana_stg_raw_swaps') }}
    WHERE 1=1
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_time') }}
        {% else -%}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif -%}
)

, transfers AS (
    SELECT
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE WHEN s.is_inner = false THEN 'direct' ELSE s.outer_executing_account END AS trade_source
        , MAX(CASE WHEN tf.inner_instruction_index = s.inner_instruction_index + 2 THEN tf.amount END) AS token_bought_amount_raw
        , MAX(CASE WHEN tf.inner_instruction_index = s.inner_instruction_index + 1 THEN tf.amount END) AS token_sold_amount_raw
        , MAX(CASE WHEN tf.inner_instruction_index = s.inner_instruction_index + 2 THEN tf.from_token_account END) AS token_bought_vault
        , MAX(CASE WHEN tf.inner_instruction_index = s.inner_instruction_index + 1 THEN tf.to_token_account END) AS token_sold_vault
        , MAX(CASE WHEN tf.inner_instruction_index = s.inner_instruction_index + 2 THEN tf.token_mint_address END) AS token_bought_mint_address
        , MAX(CASE WHEN tf.inner_instruction_index = s.inner_instruction_index + 1 THEN tf.token_mint_address END) AS token_sold_mint_address
        , s.pool_id AS project_program_id
        , s.tx_signer AS trader_id
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
        , s.surrogate_key
    FROM swaps s
    INNER JOIN {{ source('tokens_solana', 'transfers') }} tf
        ON  tf.tx_id = s.tx_id
        AND tf.block_date = s.block_date
        AND tf.block_slot = s.block_slot
        AND tf.outer_instruction_index = s.outer_instruction_index
        AND tf.inner_instruction_index IN (s.inner_instruction_index + 1, s.inner_instruction_index + 2)
    WHERE tf.token_version IN ('spl_token', 'spl_token_2022')
        {% if is_incremental() -%}
        AND {{ incremental_predicate('tf.block_date') }}
        {% else -%}
        AND tf.block_date >= DATE '{{ project_start_date }}'
        {% endif -%}
    GROUP BY
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE WHEN s.is_inner = false THEN 'direct' ELSE s.outer_executing_account END
        , s.pool_id
        , s.tx_signer
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
        , s.surrogate_key
    HAVING COUNT_IF(tf.inner_instruction_index = s.inner_instruction_index + 2) = 1
       AND COUNT_IF(tf.inner_instruction_index = s.inner_instruction_index + 1) = 1
)

SELECT
      'solana' AS blockchain
    , 'goonfi' AS project
    , 2 AS version
    , 'v2' AS version_name
    , CAST(DATE_TRUNC('month', block_date) AS DATE) AS block_month
    , block_time
    , block_slot
    , block_date
    , trade_source
    , token_bought_amount_raw
    , token_sold_amount_raw
    , CAST(NULL AS DOUBLE) AS fee_tier
    , token_bought_mint_address
    , token_sold_mint_address
    , token_bought_vault
    , token_sold_vault
    , project_program_id
    , 'goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE' AS project_main_id
    , trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
    , surrogate_key
FROM transfers
