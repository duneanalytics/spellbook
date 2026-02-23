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
    , tags = ['prod_exclude']
  )
}}

WITH raw_swaps AS (
    SELECT
          block_slot
        , block_date
        , block_time
        , COALESCE(inner_instruction_index, 0) AS inner_instruction_index
        , outer_instruction_index
        , outer_executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , account_arguments[2] AS pool_id
        , account_arguments[3] AS vault_a
        , account_arguments[4] AS vault_b
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE 1=1
        AND executing_account = '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp'
        AND tx_success = true
        AND cardinality(account_arguments) > 8
        {% if is_incremental() -%}
        AND {{ incremental_predicate('block_date') }}
        {% else -%}
        AND block_date >= DATE '2025-06-13'
        {% endif -%}
)

, swaps AS (
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
        , max(pool_id) AS pool_id
        , max(vault_a) AS vault_a
        , max(vault_b) AS vault_b
        , {{ solana_instruction_key(
              'block_slot'
            , 'tx_index'
            , 'outer_instruction_index'
            , 'inner_instruction_index'
          ) }} AS surrogate_key
    FROM raw_swaps
    GROUP BY
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
        , s.tx_index
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
