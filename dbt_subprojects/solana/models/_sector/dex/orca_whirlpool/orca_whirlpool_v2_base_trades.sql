{{
  config(
    schema = 'orca_whirlpool_v2'
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

{% set project_start_date = '2024-05-27' %}

WITH swaps AS (
    SELECT
          block_slot
        , block_date
        , block_time
        , inner_instruction_index
        , outer_instruction_index
        , outer_executing_account
        , tx_id
        , tx_signer
        , tx_index
        , whirlpool_id
        , tokenA
        , tokenAVault
        , tokenB
        , tokenBVault
        , fee_rate
        , has_memo
    FROM {{ ref('orca_whirlpool_v2_stg_swaps') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_date') }}
        {% else %}
        AND block_date >= DATE '{{ project_start_date }}'
        AND block_date < DATE '2024-06-03'
        {% endif %}
)

, swap_transfer_keys AS (
    SELECT DISTINCT
          tx_id
        , block_date
        , block_slot
        , outer_instruction_index
        , inner_instruction_index AS swap_inner_instruction_index
        , CASE WHEN has_memo THEN inner_instruction_index + 2 ELSE inner_instruction_index + 1 END AS sold_transfer_index
        , CASE WHEN has_memo THEN inner_instruction_index + 3 ELSE inner_instruction_index + 2 END AS bought_transfer_min_index
    FROM swaps
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
        AND tf.block_date < DATE '2024-06-03'
        {% endif %}
        AND EXISTS (
            SELECT 1
            FROM swap_transfer_keys sk
            WHERE
                sk.tx_id = tf.tx_id
                AND sk.block_date = tf.block_date
                AND sk.block_slot = tf.block_slot
                AND sk.outer_instruction_index = tf.outer_instruction_index
                AND (
                    tf.inner_instruction_index = sk.sold_transfer_index
                    OR tf.inner_instruction_index >= sk.bought_transfer_min_index
                )
        )
)

, transfers_filtered AS (
    SELECT
          sk.tx_id
        , sk.block_date
        , sk.block_slot
        , sk.outer_instruction_index
        , sk.swap_inner_instruction_index
        , tp.inner_instruction_index
        , CASE
            WHEN tp.inner_instruction_index = sk.sold_transfer_index THEN 2
            ELSE 1
          END AS transfer_side
        , tp.amount
        , tp.token_mint_address
    FROM swap_transfer_keys sk
    INNER JOIN transfers_pruned tp
        ON  tp.tx_id = sk.tx_id
        AND tp.block_date = sk.block_date
        AND tp.block_slot = sk.block_slot
        AND tp.outer_instruction_index = sk.outer_instruction_index
        AND (
            tp.inner_instruction_index = sk.sold_transfer_index
            OR tp.inner_instruction_index >= sk.bought_transfer_min_index
        )
)

-- Bought side (transfer_side=1) may match multiple positions; keep the lowest index
, transfers_ranked AS (
    SELECT *
    FROM (
        SELECT *
            , row_number() OVER (
                PARTITION BY tx_id, block_date, block_slot, outer_instruction_index, swap_inner_instruction_index, transfer_side
                ORDER BY inner_instruction_index ASC
              ) AS rn
        FROM transfers_filtered
    )
    WHERE rn = 1
)

, transfers AS (
    SELECT
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE
            WHEN s.outer_executing_account = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'direct'
            ELSE s.outer_executing_account
          END AS trade_source
        , max(CASE WHEN tf.transfer_side = 1 THEN tf.amount END) AS token_bought_amount_raw
        , max(CASE WHEN tf.transfer_side = 2 THEN tf.amount END) AS token_sold_amount_raw
        , max(CASE WHEN tf.transfer_side = 2 THEN tf.token_mint_address END) AS sold_token_mint
        , s.whirlpool_id
        , s.tokenA
        , s.tokenAVault
        , s.tokenB
        , s.tokenBVault
        , s.fee_rate
        , s.tx_signer AS trader_id
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
    FROM swaps s
    INNER JOIN transfers_ranked tf
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
            WHEN s.outer_executing_account = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'direct'
            ELSE s.outer_executing_account
          END
        , s.whirlpool_id
        , s.tokenA
        , s.tokenAVault
        , s.tokenB
        , s.tokenBVault
        , s.fee_rate
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
    , 'whirlpool' AS project
    , 2 AS version
    , CAST(date_trunc('month', block_time) AS DATE) AS block_month
    , block_time
    , block_slot
    , trade_source
    , token_bought_amount_raw
    , token_sold_amount_raw
    , CAST(fee_rate AS DOUBLE) / 1000000 AS fee_tier
    , CASE WHEN sold_token_mint = tokenA THEN tokenA ELSE tokenB END AS token_sold_mint_address
    , CASE WHEN sold_token_mint = tokenA THEN tokenB ELSE tokenA END AS token_bought_mint_address
    , CASE WHEN sold_token_mint = tokenA THEN tokenAVault ELSE tokenBVault END AS token_sold_vault
    , CASE WHEN sold_token_mint = tokenA THEN tokenBVault ELSE tokenAVault END AS token_bought_vault
    , whirlpool_id AS project_program_id
    , 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' AS project_main_id
    , trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
    , 1 AS recent_update
FROM transfers
