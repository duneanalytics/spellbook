{{
  config(
    schema = 'orca_whirlpool_v2'
    , alias = 'base_trades_backfill'
    , tags = ['microbatch']
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'microbatch'
    , event_time = 'block_time'
    , begin = '2024-06-05'
    , batch_size = var('orca_whirlpool_v2_batch_size', 'day')
    , lookback = 1
    , unique_key = ['block_month', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set begin = '2024-06-05' %}
{% set batch_start = model.batch.event_time_start if model.batch else begin %}
{% set batch_end = model.batch.event_time_end if model.batch else '2099-01-01' %}

WITH swaps AS (
    SELECT
          sp.block_slot
        , sp.block_date
        , sp.block_month
        , sp.block_time
        , sp.inner_instruction_index
        , sp.outer_instruction_index
        , sp.outer_executing_account
        , sp.tx_id
        , sp.tx_signer
        , sp.tx_index
        , sp.whirlpool_id
        , sp.tokenA
        , sp.tokenAVault
        , sp.tokenB
        , sp.tokenBVault
        , sp.fee_rate
        , sp.has_memo
        , sp.surrogate_key
    FROM {{ ref('orca_whirlpool_v2_stg_swaps') }} sp
    WHERE
        sp.block_time >= TIMESTAMP '{{ batch_start }}'
        AND sp.block_time < TIMESTAMP '{{ batch_end }}'
)

, transfers AS (
    SELECT
          tf.tx_id
        , tf.block_date
        , tf.block_slot
        , tf.outer_instruction_index
        , tf.inner_instruction_index
        , tf.amount
        , tf.token_mint_address
    FROM {{ source('tokens_solana', 'transfers') }} tf
    WHERE
        tf.block_time >= TIMESTAMP '{{ batch_start }}'
        AND tf.block_time < TIMESTAMP '{{ batch_end }}'
)

, all_swaps AS (
    SELECT
          sp.block_time
        , sp.block_slot
        , sp.block_month
        , sp.surrogate_key
        , CASE
            WHEN sp.outer_executing_account = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' THEN 'direct'
            ELSE sp.outer_executing_account
          END AS trade_source
        , trs_2.amount AS token_bought_amount_raw
        , trs_1.amount AS token_sold_amount_raw
        , trs_1.token_mint_address AS sold_token_mint
        , sp.whirlpool_id
        , sp.tokenA
        , sp.tokenAVault
        , sp.tokenB
        , sp.tokenBVault
        , sp.fee_rate
        , sp.tx_signer AS trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.inner_instruction_index
        , sp.tx_index
        , row_number() OVER (
            PARTITION BY sp.tx_id, sp.outer_instruction_index, sp.inner_instruction_index
            ORDER BY trs_2.inner_instruction_index ASC
          ) AS first_transfer_out
    FROM swaps sp
    INNER JOIN transfers trs_1
        ON trs_1.tx_id = sp.tx_id
        AND trs_1.block_date = sp.block_date
        AND trs_1.block_slot = sp.block_slot
        AND trs_1.outer_instruction_index = sp.outer_instruction_index
        AND trs_1.inner_instruction_index = CASE WHEN sp.has_memo THEN sp.inner_instruction_index + 2 ELSE sp.inner_instruction_index + 1 END
    INNER JOIN transfers trs_2
        ON trs_2.tx_id = sp.tx_id
        AND trs_2.block_date = sp.block_date
        AND trs_2.block_slot = sp.block_slot
        AND trs_2.outer_instruction_index = sp.outer_instruction_index
        AND trs_2.inner_instruction_index >= CASE WHEN sp.has_memo THEN sp.inner_instruction_index + 3 ELSE sp.inner_instruction_index + 2 END
)

SELECT
      'solana' AS blockchain
    , 'whirlpool' AS project
    , 2 AS version
    , tb.block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , CAST(tb.fee_rate AS DOUBLE) / 1000000 AS fee_tier
    , CASE WHEN tb.sold_token_mint = tb.tokenA THEN tb.tokenA ELSE tb.tokenB END AS token_sold_mint_address
    , CASE WHEN tb.sold_token_mint = tb.tokenA THEN tb.tokenB ELSE tb.tokenA END AS token_bought_mint_address
    , CASE WHEN tb.sold_token_mint = tb.tokenA THEN tb.tokenAVault ELSE tb.tokenBVault END AS token_sold_vault
    , CASE WHEN tb.sold_token_mint = tb.tokenA THEN tb.tokenBVault ELSE tb.tokenAVault END AS token_bought_vault
    , tb.whirlpool_id AS project_program_id
    , 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' AS project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , tb.surrogate_key
    , 1 AS recent_update
FROM all_swaps tb
WHERE tb.first_transfer_out = 1
