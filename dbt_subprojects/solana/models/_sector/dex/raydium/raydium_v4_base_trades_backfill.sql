{{
  config(
    schema = 'raydium_v4'
    , alias = 'base_trades_backfill'
    , tags = ['microbatch']
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'microbatch'
    , event_time = 'block_time'
    , begin = '2021-03-21'
    , batch_size = var('raydium_v4_batch_size', 'day')
    , lookback = 3
    , unique_key = ['block_month', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set begin = '2021-03-21' %}
{% set batch_start = model.batch.event_time_start if model.batch else begin %}
{% set batch_end = model.batch.event_time_end if model.batch else '2099-01-01' %}

WITH swaps AS (
    SELECT
          block_slot
        , block_time
        , inner_instruction_index
        , outer_instruction_index
        , outer_executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , pool_id
    FROM {{ ref('raydium_v4_solana_stg_decoded_swaps') }}
    WHERE
        block_time >= timestamp '{{ batch_start }}'
        AND block_time < timestamp '{{ batch_end }}'
)

, transfers AS (
    SELECT
          tx_id
        , outer_instruction_index
        , inner_instruction_index
        , amount
        , token_mint_address
        , from_token_account
        , to_token_account
    FROM {{ source('tokens_solana', 'transfers') }}
    WHERE
        block_time >= timestamp '{{ batch_start }}'
        AND block_time < timestamp '{{ batch_end }}'
        AND (token_version = 'spl_token' OR token_version = 'spl_token_2022')
)

, all_swaps AS (
    SELECT
          sp.block_time
        , sp.block_slot
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
        , trs_2.token_mint_address AS token_bought_mint_address
        , trs_1.token_mint_address AS token_sold_mint_address
        , trs_2.from_token_account AS token_bought_vault
        , trs_1.to_token_account AS token_sold_vault
    FROM swaps sp
    INNER JOIN transfers trs_1
        ON trs_1.tx_id = sp.tx_id
        AND trs_1.outer_instruction_index = sp.outer_instruction_index
        AND trs_1.inner_instruction_index = sp.inner_instruction_index + 1
    INNER JOIN transfers trs_2
        ON trs_2.tx_id = sp.tx_id
        AND trs_2.outer_instruction_index = sp.outer_instruction_index
        AND trs_2.inner_instruction_index = sp.inner_instruction_index + 2
)

SELECT
      'solana' AS blockchain
    , 'raydium' AS project
    , 4 AS version
    , 'amm' AS version_name
    , CAST(date_trunc('month', tb.block_time) AS DATE) AS block_month
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
    , '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' AS project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , {{ dbt_utils.generate_surrogate_key(['tb.block_slot', 'tb.tx_id', 'tb.tx_index', 'tb.outer_instruction_index', 'tb.inner_instruction_index']) }} AS surrogate_key
FROM all_swaps tb
