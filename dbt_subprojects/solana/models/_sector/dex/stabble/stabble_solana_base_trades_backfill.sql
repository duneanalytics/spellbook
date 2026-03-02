{{
  config(
    schema = 'stabble_solana'
    , alias = 'base_trades_backfill'
    , tags = ['microbatch']
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'microbatch'
    , event_time = 'block_time'
    , begin = '2024-04-01'
    , batch_size = var('stabble_batch_size', 'day')
    , lookback = 3
    , unique_key = ['block_month', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set begin = '2024-04-01' %}
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
        , token_sold_amount_raw
        , token_sold_mint_address
        , token_bought_mint_address
        , user_token_in
        , user_token_out
        , token_sold_vault
        , token_bought_vault
        , surrogate_key
    FROM {{ ref('stabble_solana_stg_decoded_swaps') }}
    WHERE
        block_time >= timestamp '{{ batch_start }}'
        AND block_time < timestamp '{{ batch_end }}'
)

, transfers AS (
    SELECT
          tx_id
        , block_date
        , block_slot
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
)

, joined AS (
    SELECT
          sp.block_time
        , sp.block_slot
        , sp.block_month
        , sp.surrogate_key
        , CASE WHEN sp.is_inner = false THEN 'direct'
            ELSE sp.outer_executing_account
          END AS trade_source
        , t_buy.amount AS token_bought_amount_raw
        , sp.token_sold_amount_raw
        , COALESCE(sp.token_sold_mint_address, t_sell.token_mint_address) AS token_sold_mint_address
        , t_buy.token_mint_address AS token_bought_mint_address
        , sp.token_sold_vault
        , sp.token_bought_vault
        , sp.pool_id
        , sp.tx_signer AS trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.inner_instruction_index
        , sp.tx_index
        , ROW_NUMBER() OVER (
            PARTITION BY sp.tx_id, sp.outer_instruction_index, sp.inner_instruction_index
            ORDER BY t_buy.amount DESC
          ) AS rn
    FROM swaps sp
    INNER JOIN transfers t_buy
        ON t_buy.tx_id = sp.tx_id
        AND t_buy.block_date = sp.block_date
        AND t_buy.block_slot = sp.block_slot
        AND t_buy.outer_instruction_index = sp.outer_instruction_index
        AND t_buy.from_token_account = sp.token_bought_vault
        AND t_buy.to_token_account = sp.user_token_out
        AND t_buy.inner_instruction_index IN (
            sp.inner_instruction_index + 1,
            sp.inner_instruction_index + 2,
            sp.inner_instruction_index + 3,
            sp.inner_instruction_index + 4
        )
    LEFT JOIN transfers t_sell
        ON t_sell.tx_id = sp.tx_id
        AND t_sell.block_date = sp.block_date
        AND t_sell.block_slot = sp.block_slot
        AND t_sell.outer_instruction_index = sp.outer_instruction_index
        AND t_sell.from_token_account = sp.user_token_in
        AND t_sell.to_token_account = sp.token_sold_vault
        AND t_sell.inner_instruction_index IN (
            sp.inner_instruction_index + 1,
            sp.inner_instruction_index + 2,
            sp.inner_instruction_index + 3,
            sp.inner_instruction_index + 4
        )
)

SELECT
      'solana' AS blockchain
    , 'stabble' AS project
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
    , 'vo1tWgqZMjG61Z2T9qUaMYKqZ75CYzMuaZ2LZP1n7HV' AS project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , tb.surrogate_key
FROM joined tb
WHERE tb.rn = 1
