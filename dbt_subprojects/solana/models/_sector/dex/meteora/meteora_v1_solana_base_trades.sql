{{
  config(
    schema = 'meteora_v1_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index', 'block_month']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2022-07-27' %}

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
        , deposit_index
    FROM {{ ref('meteora_v1_stg_swaps') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_date') }}
        {% else %}
        AND block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

, swap_transfer_keys AS (
    SELECT DISTINCT
          tx_id
        , block_date
        , block_slot
        , outer_instruction_index
        , inner_instruction_index AS swap_inner_instruction_index
        , transfer_inner_instruction_index
        , transfer_side
    FROM (
        SELECT
              tx_id
            , block_date
            , block_slot
            , outer_instruction_index
            , inner_instruction_index
            , deposit_index + 4 AS transfer_inner_instruction_index
            , 1 AS transfer_side
        FROM swaps

        UNION ALL

        SELECT
              tx_id
            , block_date
            , block_slot
            , outer_instruction_index
            , inner_instruction_index
            , deposit_index + 1 AS transfer_inner_instruction_index
            , 2 AS transfer_side
        FROM swaps
    )
)

, swap_slots AS (
    SELECT DISTINCT block_date, block_slot
    FROM swap_transfer_keys
)

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
    INNER JOIN swap_slots ss
        ON  ss.block_date = tf.block_date
        AND ss.block_slot = tf.block_slot
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('tf.block_date') }}
        {% else %}
        AND tf.block_date >= DATE '{{ project_start_date }}'
        {% endif %}
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

, transfers_filtered AS (
    SELECT
          sk.tx_id
        , sk.block_date
        , sk.block_slot
        , sk.outer_instruction_index
        , sk.swap_inner_instruction_index
        , sk.transfer_inner_instruction_index AS inner_instruction_index
        , sk.transfer_side
        , tp.amount
        , tp.from_token_account
        , tp.to_token_account
        , tp.token_mint_address
    FROM swap_transfer_keys sk
    INNER JOIN transfers_pruned tp
        ON  tp.tx_id = sk.tx_id
        AND tp.block_date = sk.block_date
        AND tp.block_slot = sk.block_slot
        AND tp.outer_instruction_index = sk.outer_instruction_index
        AND tp.inner_instruction_index = sk.transfer_inner_instruction_index
)

, transfers AS (
    SELECT
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE
            WHEN s.is_inner = false THEN 'direct'
            ELSE s.outer_executing_account
          END AS trade_source
        , max(CASE WHEN tf.transfer_side = 1 THEN tf.amount END) AS token_bought_amount_raw
        , max(CASE WHEN tf.transfer_side = 2 THEN tf.amount END) AS token_sold_amount_raw
        , max(CASE WHEN tf.transfer_side = 1 THEN tf.from_token_account END) AS token_bought_vault
        , max(CASE WHEN tf.transfer_side = 2 THEN tf.to_token_account END) AS token_sold_vault
        , max(CASE WHEN tf.transfer_side = 1 THEN tf.token_mint_address END) AS token_bought_mint_address
        , max(CASE WHEN tf.transfer_side = 2 THEN tf.token_mint_address END) AS token_sold_mint_address
        , s.pool_id AS project_program_id
        , s.tx_signer AS trader_id
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
    FROM swaps s
    INNER JOIN transfers_filtered tf
        ON  tf.tx_id = s.tx_id
        AND tf.block_date = s.block_date
        AND tf.block_slot = s.block_slot
        AND tf.outer_instruction_index = s.outer_instruction_index
        AND tf.swap_inner_instruction_index = s.inner_instruction_index
    GROUP BY
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE
            WHEN s.is_inner = false THEN 'direct'
            ELSE s.outer_executing_account
          END
        , s.pool_id
        , s.tx_signer
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
    HAVING 1=1
        AND count_if(tf.transfer_side = 1) = 1
        AND count_if(tf.transfer_side = 2) = 1
)

SELECT
      'solana' AS blockchain
    , 'meteora' AS project
    , 1 AS version
    , CAST(date_trunc('month', block_time) AS DATE) AS block_month
    , block_time
    , block_slot
    , trade_source
    , token_bought_amount_raw
    , token_sold_amount_raw
    , CAST(NULL AS DOUBLE) AS fee_tier
    , token_sold_mint_address
    , token_bought_mint_address
    , token_sold_vault
    , token_bought_vault
    , project_program_id
    , 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' AS project_main_id
    , trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
FROM transfers
