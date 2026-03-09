{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'base_trades_backfill'
    , tags = ['microbatch']
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'microbatch'
    , event_time = 'block_time'
    , begin = '2025-02-20'
    , batch_size = var('pumpswap_batch_size', 'day')
    , lookback = 1
    , unique_key = ['block_month', 'surrogate_key']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set begin = '2026-03-06' %}
{% set batch_start = model.batch.event_time_start if model.batch else begin %}
{% set batch_end = model.batch.event_time_end if model.batch else '2099-01-01' %}

WITH pools AS (
    SELECT
          pool
        , baseMint
        , quoteMint
    FROM {{ ref('pumpswap_solana_pools') }}
)

, swaps AS (
    SELECT
          block_slot
        , block_date
        , block_month
        , block_time
        , inner_instruction_index
        , swap_inner_index
        , outer_instruction_index
        , outer_executing_account
        , tx_id
        , tx_index
        , pool
        , user_account
        , account_user_base_token_account
        , account_user_quote_token_account
        , account_pool_base_token_account
        , account_pool_quote_token_account
        , account_protocol_fee_recipient_token_account
        , base_amount
        , is_buy
        , surrogate_key
    FROM {{ ref('pumpswap_solana_int_all_swaps') }}
    WHERE
        block_time >= TIMESTAMP '{{ batch_start }}'
        AND block_time < TIMESTAMP '{{ batch_end }}'
)

, fee_configs_with_time_ranges AS (
    SELECT
          call_block_time AS start_time
        , LEAD(call_block_time) OVER (ORDER BY call_block_time) AS end_time
        , (lp_fee_basis_points + protocol_fee_basis_points) / 10000.0 AS total_fee_rate
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_update_fee_config') }}
)

, fee_configs AS (
    SELECT
          start_time
        , COALESCE(end_time, TIMESTAMP '2099-12-31') AS end_time
        , total_fee_rate
    FROM fee_configs_with_time_ranges
)

, transfers AS (
    SELECT
          tx_id
        , block_slot
        , outer_instruction_index
        , inner_instruction_index
        , amount
        , from_token_account
        , to_token_account
    FROM {{ source('tokens_solana', 'transfers') }}
    WHERE
        block_time >= TIMESTAMP '{{ batch_start }}'
        AND block_time < TIMESTAMP '{{ batch_end }}'
)

, swaps_with_fees AS (
    SELECT
          s.*
        , COALESCE(f.total_fee_rate, 0.0025) AS total_fee_rate
    FROM swaps s
    LEFT JOIN fee_configs f
        ON s.block_time >= f.start_time
        AND s.block_time < f.end_time
)

, swaps_with_transfers AS (
    SELECT
          sf.*
        , sf.base_amount AS base_token_amount
        , t.amount AS quote_token_amount
        , ROW_NUMBER() OVER (
            PARTITION BY sf.tx_id, sf.outer_instruction_index, sf.swap_inner_index
            ORDER BY t.inner_instruction_index ASC
          ) AS rn
    FROM swaps_with_fees sf
    INNER JOIN transfers t
        ON t.tx_id = sf.tx_id
        AND t.block_slot = sf.block_slot
        AND t.outer_instruction_index = sf.outer_instruction_index
        AND t.to_token_account != sf.account_protocol_fee_recipient_token_account
        AND (
            (sf.swap_inner_index IS NULL
                AND t.inner_instruction_index BETWEEN 1 AND 12
                AND CASE
                        WHEN sf.is_buy = 1 THEN t.from_token_account = sf.account_user_quote_token_account
                        ELSE t.from_token_account = sf.account_pool_quote_token_account
                    END
            )
            OR
            (sf.swap_inner_index IS NOT NULL
                AND t.inner_instruction_index BETWEEN sf.swap_inner_index + 1 AND sf.swap_inner_index + 12
                AND CASE
                        WHEN sf.is_buy = 1 THEN t.from_token_account = sf.account_user_quote_token_account
                        ELSE t.from_token_account = sf.account_pool_quote_token_account
                    END
            )
        )
)

, trades AS (
    SELECT
          sp.block_time
        , sp.block_slot
        , sp.block_month
        , sp.surrogate_key
        , CASE
            WHEN sp.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' THEN 'direct'
            ELSE sp.outer_executing_account
          END AS trade_source
        , CASE WHEN sp.is_buy = 1 THEN p.baseMint ELSE p.quoteMint END AS token_bought_mint_address
        , CASE WHEN sp.is_buy = 1 THEN sp.base_token_amount ELSE sp.quote_token_amount END AS token_bought_amount_raw
        , CASE WHEN sp.is_buy = 0 THEN p.baseMint ELSE p.quoteMint END AS token_sold_mint_address
        , CASE WHEN sp.is_buy = 0 THEN sp.base_token_amount ELSE sp.quote_token_amount END AS token_sold_amount_raw
        , CAST(sp.total_fee_rate AS DOUBLE) AS fee_tier
        , sp.pool AS pool_id
        , sp.user_account AS trader_id
        , sp.tx_id
        , sp.outer_instruction_index
        , sp.inner_instruction_index
        , sp.tx_index
        , CASE WHEN sp.is_buy = 1 THEN sp.account_pool_base_token_account ELSE sp.account_pool_quote_token_account END AS token_bought_vault
        , CASE WHEN sp.is_buy = 1 THEN sp.account_pool_quote_token_account ELSE sp.account_pool_base_token_account END AS token_sold_vault
    FROM swaps_with_transfers sp
    LEFT JOIN pools p ON p.pool = sp.pool
    WHERE sp.rn = 1
)

SELECT
      'solana' AS blockchain
    , 'pumpswap' AS project
    , 1 AS version
    , tb.block_month
    , tb.block_time
    , tb.block_slot
    , tb.trade_source
    , tb.token_bought_amount_raw
    , tb.token_sold_amount_raw
    , tb.fee_tier
    , tb.token_sold_mint_address
    , tb.token_bought_mint_address
    , tb.token_sold_vault
    , tb.token_bought_vault
    , tb.pool_id AS project_program_id
    , 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' AS project_main_id
    , tb.trader_id
    , tb.tx_id
    , tb.outer_instruction_index
    , tb.inner_instruction_index
    , tb.tx_index
    , tb.surrogate_key
FROM trades tb
