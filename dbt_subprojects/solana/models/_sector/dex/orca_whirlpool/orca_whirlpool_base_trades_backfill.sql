{{
  config(
    schema = 'orca_whirlpool'
    , alias = 'base_trades_backfill'
    , tags = ['microbatch']
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'microbatch'
    , event_time = 'block_time'
    , begin = '2022-04-06'
    , batch_size = var('orca_whirlpool_batch_size', 'day')
    , lookback = 1
    , unique_key = ['block_month', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set begin = '2022-04-06' %}
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
        , sp.pool_id
        , sp.fee_rate
        , sp.surrogate_key
    FROM {{ ref('orca_whirlpool_stg_swaps') }} sp
    WHERE
        sp.block_time >= timestamp '{{ batch_start }}'
        AND sp.block_time < timestamp '{{ batch_end }}'
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
        , tf.from_token_account
        , tf.to_token_account
    FROM {{ source('tokens_solana', 'transfers') }} tf
    WHERE
        tf.block_time >= timestamp '{{ batch_start }}'
        AND tf.block_time < timestamp '{{ batch_end }}'
        AND tf.token_version = 'spl_token'
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
        , sp.pool_id
        , sp.fee_rate
        , sp.tx_signer AS trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.inner_instruction_index
        , sp.tx_index
        , trs_2.token_mint_address AS token_bought_mint_address
        , trs_1.token_mint_address AS token_sold_mint_address
        , trs_2.from_token_account AS token_bought_vault
        , trs_1.to_token_account AS token_sold_vault
    FROM swaps sp
    INNER JOIN transfers trs_1
        ON trs_1.tx_id = sp.tx_id
        AND trs_1.block_date = sp.block_date
        AND trs_1.block_slot = sp.block_slot
        AND trs_1.outer_instruction_index = sp.outer_instruction_index
        AND trs_1.inner_instruction_index = sp.inner_instruction_index + 1
    INNER JOIN transfers trs_2
        ON trs_2.tx_id = sp.tx_id
        AND trs_2.block_date = sp.block_date
        AND trs_2.block_slot = sp.block_slot
        AND trs_2.outer_instruction_index = sp.outer_instruction_index
        AND trs_2.inner_instruction_index = sp.inner_instruction_index + 2
)

SELECT
      'solana' AS blockchain
    , 'whirlpool' AS project
    , 1 AS version
    , tb.block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , CAST(tb.fee_rate AS DOUBLE) / 1000000 AS fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id AS project_program_id
    , 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' AS project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , tb.surrogate_key
FROM all_swaps tb
