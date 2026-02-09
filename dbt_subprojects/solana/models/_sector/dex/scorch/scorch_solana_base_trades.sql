{{
  config(
    schema = 'scorch_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
    , pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-11-28' %}

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
        , surrogate_key
    FROM {{ ref('scorch_solana_stg_raw_swaps') }}
    WHERE 1=1
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_date') }}
        {% else -%}
        AND block_date >= DATE '{{ project_start_date }}'
        AND block_date < DATE '{{ project_start_date }}' + INTERVAL '1' DAY
        {% endif -%}
)

-- Step 1: compute expected transfer positions for semi-join pruning
, swap_transfer_keys AS (
    SELECT DISTINCT
          tx_id
        , block_date
        , block_slot
        , outer_instruction_index
        , transfer_inner_instruction_index
    FROM (
        SELECT
              tx_id
            , block_date
            , block_slot
            , outer_instruction_index
            , inner_instruction_index + 2 AS transfer_inner_instruction_index
        FROM swaps

        UNION ALL

        SELECT
              tx_id
            , block_date
            , block_slot
            , outer_instruction_index
            , inner_instruction_index + 3 AS transfer_inner_instruction_index
        FROM swaps
    )
)

-- Step 2: filter transfers using a SEMI join (EXISTS) so the hash build is on swap_transfer_keys, not transfers
, transfers_pruned AS (
    SELECT
          tf.tx_id
        , tf.block_date
        , tf.block_slot
        , tf.outer_instruction_index
        , tf.inner_instruction_index
        , tf.amount
        , tf.from_token_account
        , tf.to_token_account
        , tf.token_mint_address
    FROM {{ source('tokens_solana', 'transfers') }} tf
    WHERE
        1=1
        AND tf.token_version IN ('spl_token', 'spl_token_2022')
        {% if is_incremental() -%}
        AND {{ incremental_predicate('tf.block_date') }}
        {% else -%}
        AND tf.block_date >= DATE '{{ project_start_date }}'
        AND tf.block_date < DATE '{{ project_start_date }}' + INTERVAL '1' DAY
        {% endif -%}
        AND EXISTS (
            SELECT 1
            FROM swap_transfer_keys sk
            WHERE
                sk.tx_id = tf.tx_id
                AND sk.block_date = tf.block_date
                AND sk.block_slot = tf.block_slot
                AND sk.outer_instruction_index = tf.outer_instruction_index
                AND sk.transfer_inner_instruction_index = tf.inner_instruction_index
        )
)

-- Step 3: join pruned transfers with swaps
, transfers AS (
    SELECT
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE WHEN s.is_inner = false THEN 'direct' ELSE s.outer_executing_account END AS trade_source
        , MAX(CASE WHEN tp.inner_instruction_index = s.inner_instruction_index + 3 THEN tp.amount END) AS token_bought_amount_raw
        , MAX(CASE WHEN tp.inner_instruction_index = s.inner_instruction_index + 2 THEN tp.amount END) AS token_sold_amount_raw
        , MAX(CASE WHEN tp.inner_instruction_index = s.inner_instruction_index + 3 THEN tp.from_token_account END) AS token_bought_vault
        , MAX(CASE WHEN tp.inner_instruction_index = s.inner_instruction_index + 2 THEN tp.to_token_account END) AS token_sold_vault
        , MAX(CASE WHEN tp.inner_instruction_index = s.inner_instruction_index + 3 THEN tp.token_mint_address END) AS token_bought_mint_address
        , MAX(CASE WHEN tp.inner_instruction_index = s.inner_instruction_index + 2 THEN tp.token_mint_address END) AS token_sold_mint_address
        , s.pool_id AS project_program_id
        , s.tx_signer AS trader_id
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
        , s.surrogate_key
    FROM swaps s
    INNER JOIN transfers_pruned tp
        ON  tp.tx_id = s.tx_id
        AND tp.block_date = s.block_date
        AND tp.block_slot = s.block_slot
        AND tp.outer_instruction_index = s.outer_instruction_index
        AND tp.inner_instruction_index IN (s.inner_instruction_index + 2, s.inner_instruction_index + 3)
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
    HAVING COUNT_IF(tp.inner_instruction_index = s.inner_instruction_index + 3) = 1
       AND COUNT_IF(tp.inner_instruction_index = s.inner_instruction_index + 2) = 1
)

SELECT
      'solana' AS blockchain
    , 'scorch' AS project
    , 1 AS version
    , 'v1' AS version_name
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
    , 'SCoRcH8c2dpjvcJD6FiPbCSQyQgu3PcUAWj2Xxx3mqn' AS project_main_id
    , trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
    , surrogate_key
FROM transfers
