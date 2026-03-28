{{
  config(
    schema = 'manifest_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'tx_id', 'outer_instruction_index', 'inner_instruction_index', 'tx_index']
    , pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

WITH swaps AS (
    SELECT
          s.block_slot
        , s.block_month
        , s.block_date
        , s.block_time
        , s.inner_instruction_index
        , s.outer_instruction_index
        , s.outer_executing_account
        , s.tx_id
        , s.tx_signer
        , s.tx_index
        , s.vault_a
        , s.vault_b
        , s.vault_c
        , s.disc
    FROM {{ ref('manifest_solana_stg_raw_swaps') }} s
    WHERE 1=1
        {% if is_incremental() -%}
        AND {{ incremental_predicate('s.block_date') }}
        {% else -%}
        AND s.block_date >= DATE '2025-07-31'
        {% endif -%}
)

, matched AS (
    SELECT
          s.block_time
        , s.block_slot
        , s.block_month
        , s.block_date
        , s.tx_id
        , s.tx_index
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.outer_executing_account
        , s.tx_signer
        , MAX_BY(
            CASE
                WHEN s.disc = 0x04 AND tf.from_token_account IN (s.vault_a, s.vault_b) THEN tf.amount
                WHEN s.disc = 0x0d AND tf.from_token_account IN (s.vault_b, s.vault_c) THEN tf.amount
            END,
            CASE
                WHEN s.disc = 0x04 AND tf.from_token_account IN (s.vault_a, s.vault_b) THEN tf.amount
                WHEN s.disc = 0x0d AND tf.from_token_account IN (s.vault_b, s.vault_c) THEN tf.amount
            END
          ) AS token_bought_amount_raw
        , MAX_BY(
            CASE
                WHEN s.disc = 0x04 AND tf.to_token_account IN (s.vault_a, s.vault_b) THEN tf.amount
                WHEN s.disc = 0x0d AND tf.to_token_account IN (s.vault_b, s.vault_c) THEN tf.amount
            END,
            CASE
                WHEN s.disc = 0x04 AND tf.to_token_account IN (s.vault_a, s.vault_b) THEN tf.amount
                WHEN s.disc = 0x0d AND tf.to_token_account IN (s.vault_b, s.vault_c) THEN tf.amount
            END
          ) AS token_sold_amount_raw
        , MAX_BY(
            CASE
                WHEN s.disc = 0x04 AND tf.from_token_account IN (s.vault_a, s.vault_b) THEN tf.from_token_account
                WHEN s.disc = 0x0d AND tf.from_token_account IN (s.vault_b, s.vault_c) THEN tf.from_token_account
            END,
            CASE
                WHEN s.disc = 0x04 AND tf.from_token_account IN (s.vault_a, s.vault_b) THEN tf.amount
                WHEN s.disc = 0x0d AND tf.from_token_account IN (s.vault_b, s.vault_c) THEN tf.amount
            END
          ) AS token_bought_vault
        , MAX_BY(
            CASE
                WHEN s.disc = 0x04 AND tf.to_token_account IN (s.vault_a, s.vault_b) THEN tf.to_token_account
                WHEN s.disc = 0x0d AND tf.to_token_account IN (s.vault_b, s.vault_c) THEN tf.to_token_account
            END,
            CASE
                WHEN s.disc = 0x04 AND tf.to_token_account IN (s.vault_a, s.vault_b) THEN tf.amount
                WHEN s.disc = 0x0d AND tf.to_token_account IN (s.vault_b, s.vault_c) THEN tf.amount
            END
          ) AS token_sold_vault
        , MAX_BY(
            CASE
                WHEN s.disc = 0x04 AND tf.from_token_account IN (s.vault_a, s.vault_b) THEN tf.token_mint_address
                WHEN s.disc = 0x0d AND tf.from_token_account IN (s.vault_b, s.vault_c) THEN tf.token_mint_address
            END,
            CASE
                WHEN s.disc = 0x04 AND tf.from_token_account IN (s.vault_a, s.vault_b) THEN tf.amount
                WHEN s.disc = 0x0d AND tf.from_token_account IN (s.vault_b, s.vault_c) THEN tf.amount
            END
          ) AS token_bought_mint_address
        , MAX_BY(
            CASE
                WHEN s.disc = 0x04 AND tf.to_token_account IN (s.vault_a, s.vault_b) THEN tf.token_mint_address
                WHEN s.disc = 0x0d AND tf.to_token_account IN (s.vault_b, s.vault_c) THEN tf.token_mint_address
            END,
            CASE
                WHEN s.disc = 0x04 AND tf.to_token_account IN (s.vault_a, s.vault_b) THEN tf.amount
                WHEN s.disc = 0x0d AND tf.to_token_account IN (s.vault_b, s.vault_c) THEN tf.amount
            END
          ) AS token_sold_mint_address
    FROM swaps s
    INNER JOIN {{ source('tokens_solana', 'transfers') }} tf
        ON tf.block_slot = s.block_slot
        AND tf.tx_index = s.tx_index
        AND tf.outer_instruction_index = s.outer_instruction_index
        AND tf.inner_instruction_index BETWEEN s.inner_instruction_index + 1 AND s.inner_instruction_index + 4
    WHERE 1=1
        {% if is_incremental() -%}
        AND {{ incremental_predicate('tf.block_date') }}
        {% else -%}
        AND tf.block_date >= DATE '2025-07-31'
        {% endif -%}
    GROUP BY
          s.block_time
        , s.block_slot
        , s.block_month
        , s.block_date
        , s.tx_id
        , s.tx_index
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.outer_executing_account
        , s.tx_signer
    HAVING COUNT(DISTINCT tf.token_mint_address) = 2
)

SELECT
      'solana' AS blockchain
    , 'manifest' AS project
    , 1 AS version
    , 'v1' AS version_name
    , block_month
    , block_time
    , block_slot
    , block_date
    , outer_executing_account AS trade_source
    , token_bought_amount_raw
    , token_sold_amount_raw
    , CAST(NULL AS DOUBLE) AS fee_tier
    , token_bought_mint_address
    , token_sold_mint_address
    , token_bought_vault
    , token_sold_vault
    , CAST(NULL AS VARCHAR) AS project_program_id
    , 'MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms' AS project_main_id
    , tx_signer AS trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
FROM matched
