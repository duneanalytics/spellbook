{{
  config(
    schema = 'pumpswap_solana'
    , alias = 'buy_exact_quote_in_base_trades_backfill'
    , tags = ['microbatch']
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'microbatch'
    , event_time = 'block_time'
    , begin = '2025-11-01'
    , batch_size = 'month'
    , lookback = 1
    , unique_key = ['block_month', 'surrogate_key']
  )
}}

{% set begin = '2025-11-01' %}
{% set batch_start = model.batch.event_time_start if model.batch else begin %}
{% set batch_end = model.batch.event_time_end if model.batch else '2099-01-01' %}

WITH pools AS (
    SELECT
          pool
        , baseMint
        , quoteMint
        , is_valid_pool
    FROM {{ ref('pumpswap_solana_pools') }}
)

, pool_accounts AS (
    SELECT
          account_pool AS pool
        , arbitrary(account_pool_base_token_account) AS pool_base_token_account
        , arbitrary(account_pool_quote_token_account) AS pool_quote_token_account
    FROM {{ source('pumpdotfun_solana', 'pump_amm_call_create_pool') }}
    WHERE call_block_time >= TIMESTAMP '2025-02-20'
    GROUP BY 1
)

, event_only_buys AS (
    SELECT
          e.evt_block_slot AS block_slot
        , e.evt_block_date AS block_date
        , CAST(date_trunc('month', e.evt_block_date) AS DATE) AS block_month
        , e.evt_block_time AS block_time
        , e.evt_inner_instruction_index AS inner_instruction_index
        , e.evt_outer_instruction_index AS outer_instruction_index
        , e.evt_outer_executing_account AS outer_executing_account
        , e.evt_tx_id AS tx_id
        , e.evt_tx_index AS tx_index
        , e.pool
        , e.user AS user_account
        , e.user_base_token_account AS account_user_base_token_account
        , e.user_quote_token_account AS account_user_quote_token_account
        , pa.pool_base_token_account AS account_pool_base_token_account
        , pa.pool_quote_token_account AS account_pool_quote_token_account
        , e.protocol_fee_recipient_token_account AS account_protocol_fee_recipient_token_account
        , e.base_amount_out AS base_amount
        , e.quote_amount_in AS quote_amount
        , CAST((e.lp_fee_basis_points + e.protocol_fee_basis_points) AS DOUBLE) / 10000.0 AS fee_tier
        , {{ solana_instruction_key(
              'e.evt_block_slot'
            , 'e.evt_tx_index'
            , 'e.evt_outer_instruction_index'
            , 'COALESCE(e.evt_inner_instruction_index, 0)'
          ) }} AS surrogate_key
    FROM {{ source('pumpdotfun_solana', 'pump_amm_evt_buyevent') }} e
    INNER JOIN pool_accounts pa
        ON pa.pool = e.pool
    LEFT JOIN {{ source('pumpdotfun_solana', 'pump_amm_call_buy') }} c
        ON c.call_block_date = e.evt_block_date
        AND c.call_tx_id = e.evt_tx_id
        AND c.call_outer_instruction_index = e.evt_outer_instruction_index
        AND c.account_pool = e.pool
        AND c.base_amount_out = e.base_amount_out
        AND c.call_block_date >= DATE '{{ begin }}'
    WHERE c.call_tx_id IS NULL
        AND e.evt_block_time >= TIMESTAMP '{{ batch_start }}'
        AND e.evt_block_time < TIMESTAMP '{{ batch_end }}'
)

SELECT
      'solana' AS blockchain
    , 'pumpswap' AS project
    , 1 AS version
    , b.block_month
    , b.block_time
    , b.block_slot
    , CASE
        WHEN b.outer_executing_account = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' THEN 'direct'
        ELSE b.outer_executing_account
      END AS trade_source
    , b.base_amount AS token_bought_amount_raw
    , b.quote_amount AS token_sold_amount_raw
    , b.fee_tier
    , p.quoteMint AS token_sold_mint_address
    , p.baseMint AS token_bought_mint_address
    , b.account_pool_quote_token_account AS token_sold_vault
    , b.account_pool_base_token_account AS token_bought_vault
    , b.pool AS project_program_id
    , 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' AS project_main_id
    , b.user_account AS trader_id
    , b.tx_id
    , b.outer_instruction_index
    , b.inner_instruction_index
    , b.tx_index
    , b.surrogate_key
FROM event_only_buys b
LEFT JOIN pools p ON p.pool = b.pool
WHERE COALESCE(p.is_valid_pool, false)
