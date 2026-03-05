{{
 config(
       schema = 'meteora_v1_solana',
       alias = 'base_trades_backfill',
       tags = ['microbatch'],
       partition_by = ['block_month'],
       materialized = 'incremental',
       file_format = 'delta',
       incremental_strategy = 'microbatch',
       event_time = 'block_time',
       begin = '2022-07-27',
       batch_size = var('meteora_v1_batch_size', 'day'),
       lookback = 1,
       unique_key = ['block_month', 'surrogate_key'],
       pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
       )
}}

{% set begin = '2022-07-27' %}
{% set batch_start = model.batch.event_time_start if model.batch else begin %}
{% set batch_end = model.batch.event_time_end if model.batch else '2099-01-01' %}

WITH swaps AS (
    SELECT
          block_slot
        , block_date
        , block_month
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
        , surrogate_key
    FROM {{ ref('meteora_v1_solana_stg_swaps') }}
    WHERE
        block_time >= TIMESTAMP '{{ batch_start }}'
        AND block_time < TIMESTAMP '{{ batch_end }}'
)

, transfers AS (
    SELECT
          tx_id
        , block_date
        , block_time
        , outer_instruction_index
        , inner_instruction_index
        , amount
        , token_mint_address
        , from_token_account
        , to_token_account
    FROM {{ source('tokens_solana', 'transfers') }}
    WHERE
        block_time >= TIMESTAMP '{{ batch_start }}'
        AND block_time < TIMESTAMP '{{ batch_end }}'
)

, all_swaps AS (
    SELECT
          sp.block_time
        , sp.block_slot
        , sp.block_month
        , sp.surrogate_key
        , CASE WHEN sp.is_inner = false THEN 'direct'
            ELSE sp.outer_executing_account
          END AS trade_source
        , trs_2.amount AS token_bought_amount_raw
        , trs_1.amount AS token_sold_amount_raw
        , sp.pool_id
        , sp.tx_signer AS trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.inner_instruction_index
        , sp.tx_index
        , COALESCE(trs_2.token_mint_address, CAST(NULL AS VARCHAR)) AS token_bought_mint_address
        , COALESCE(trs_1.token_mint_address, CAST(NULL AS VARCHAR)) AS token_sold_mint_address
        , trs_2.from_token_account AS token_bought_vault
        , trs_1.to_token_account AS token_sold_vault
    FROM swaps sp
    INNER JOIN transfers trs_1
        ON trs_1.tx_id = sp.tx_id
        AND trs_1.block_date = sp.block_date
        AND trs_1.block_time = sp.block_time
        AND trs_1.outer_instruction_index = sp.outer_instruction_index
        AND trs_1.inner_instruction_index = sp.deposit_index + 1
    INNER JOIN transfers trs_2
        ON trs_2.tx_id = sp.tx_id
        AND trs_2.block_date = sp.block_date
        AND trs_2.block_time = sp.block_time
        AND trs_2.outer_instruction_index = sp.outer_instruction_index
        AND trs_2.inner_instruction_index = sp.deposit_index + 4
)

SELECT
      'solana' AS blockchain
    , 'meteora' AS project
    , 1 AS version
    , tb.block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , CAST(NULL AS DOUBLE) AS fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id AS project_program_id
    , 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' AS project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , tb.surrogate_key
FROM all_swaps tb
