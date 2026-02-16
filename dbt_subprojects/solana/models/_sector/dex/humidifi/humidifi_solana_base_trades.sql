{{
  config(
    schema = 'humidifi_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{# In GitHub Actions CI, limit to last 7 days from today; else use var (default 0 = no cap). #}
{% set initial_run_days = (7 if env_var('GITHUB_ACTIONS', '') == 'true' else (var('solana_amm_initial_run_days', 0) | int)) %}

WITH swaps AS (
    SELECT
          s.block_slot
        , s.block_date
        , s.block_time
        , s.inner_instruction_index
        , s.outer_instruction_index
        , s.outer_executing_account
        , s.is_inner
        , s.tx_id
        , s.tx_signer
        , s.tx_index
        , s.pool_id
        , ic.account_arguments[3] AS vault_a
        , ic.account_arguments[4] AS vault_b
        , s.surrogate_key
    FROM {{ ref('humidifi_solana_stg_raw_swaps') }} s
    INNER JOIN {{ source('solana', 'instruction_calls') }} ic
        ON  ic.tx_id = s.tx_id
        AND ic.block_date = s.block_date
        AND ic.block_slot = s.block_slot
        AND ic.outer_instruction_index = s.outer_instruction_index
        AND COALESCE(ic.inner_instruction_index, 0) = s.inner_instruction_index
        AND ic.executing_account = '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'
        AND ic.tx_success = true
        AND cardinality(ic.account_arguments) > 8
    WHERE 1=1
        {% if is_incremental() -%}
        AND {{ incremental_predicate('s.block_date') }}
        {% else -%}
        AND s.block_date >= DATE '2025-06-13'
        {% if initial_run_days > 0 -%}
        AND s.block_date > current_date - INTERVAL '1' DAY * {{ initial_run_days }}
        {% endif -%}
        {% endif -%}
)

, swap_slots AS (
    SELECT DISTINCT block_date, block_slot
    FROM swaps
)

, transfers_raw AS (
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
        AND tf.token_version IN ('spl_token', 'spl_token_2022')
        {% if is_incremental() -%}
        AND {{ incremental_predicate('tf.block_date') }}
        {% else -%}
        AND tf.block_date >= DATE '2025-06-13'
        {% if initial_run_days > 0 -%}
        AND tf.block_date > current_date - INTERVAL '1' DAY * {{ initial_run_days }}
        {% endif -%}
        {% endif -%}
)

, transfers_labeled AS (
    SELECT
          s.tx_id
        , s.block_date
        , s.block_slot
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.vault_a
        , s.vault_b
        , s.block_time
        , s.outer_executing_account
        , s.is_inner
        , s.pool_id
        , s.tx_signer
        , s.surrogate_key
        , tf.amount
        , tf.from_token_account
        , tf.to_token_account
        , tf.token_mint_address
        , CASE
            WHEN tf.from_token_account IN (s.vault_a, s.vault_b) THEN 'buy'
            WHEN tf.to_token_account IN (s.vault_a, s.vault_b) THEN 'sell'
          END AS transfer_type
    FROM swaps s
    INNER JOIN transfers_raw tf
        ON  tf.tx_id = s.tx_id
        AND tf.block_date = s.block_date
        AND tf.block_slot = s.block_slot
        AND tf.outer_instruction_index = s.outer_instruction_index
        AND tf.inner_instruction_index IN (s.inner_instruction_index + 1, s.inner_instruction_index + 2, s.inner_instruction_index + 3)
    WHERE (tf.from_token_account IN (s.vault_a, s.vault_b) OR tf.to_token_account IN (s.vault_a, s.vault_b))
)

, transfers AS (
    SELECT
          block_date
        , block_time
        , block_slot
        , CASE
            WHEN is_inner = false THEN 'direct'
            ELSE outer_executing_account
          END AS trade_source
        , max(CASE WHEN transfer_type = 'buy' THEN amount END) AS token_bought_amount_raw
        , max(CASE WHEN transfer_type = 'sell' THEN amount END) AS token_sold_amount_raw
        , max(CASE WHEN transfer_type = 'buy' THEN from_token_account END) AS token_bought_vault
        , max(CASE WHEN transfer_type = 'sell' THEN to_token_account END) AS token_sold_vault
        , max(CASE WHEN transfer_type = 'buy' THEN token_mint_address END) AS token_bought_mint_address
        , max(CASE WHEN transfer_type = 'sell' THEN token_mint_address END) AS token_sold_mint_address
        , pool_id AS project_program_id
        , tx_signer AS trader_id
        , tx_id
        , outer_instruction_index
        , inner_instruction_index
        , tx_index
        , surrogate_key
    FROM transfers_labeled
    GROUP BY
          block_date
        , block_time
        , block_slot
        , CASE
            WHEN is_inner = false THEN 'direct'
            ELSE outer_executing_account
          END
        , pool_id
        , tx_signer
        , tx_id
        , outer_instruction_index
        , inner_instruction_index
        , tx_index
        , surrogate_key
    HAVING 1=1
        AND count_if(transfer_type = 'buy') BETWEEN 1 AND 2
        AND count_if(transfer_type = 'sell') BETWEEN 1 AND 2
)

SELECT
      'solana' AS blockchain
    , 'humidifi' AS project
    , 1 AS version
    , 'v1' AS version_name
    , CAST(date_trunc('month', block_date) AS DATE) AS block_month
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
    , '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp' AS project_main_id
    , trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
    , surrogate_key
FROM transfers
