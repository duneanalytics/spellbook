{{
  config(
    schema = 'jupiter_lend_solana'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{% set project_start_date = '2026-06-22' %}
{% set program_id = 'jupZ4m2GqUCJ5iueMfzQf8khFfH31d4XAQt3RzCT9Vd' %}

/*
  Transfers sit at inner_instruction_index + 2 (paid into a pool vault) and + 5
  (paid out of a pool vault) for 99.55% of swaps, and at + 3 / + 6 for the rest.
  Matching on vault membership across both offset pairs covers every swap with
  exactly one transfer per side, so the offsets are accepted as a set rather than
  a fixed pair.
*/

WITH swaps AS (
    SELECT
          block_slot
        , block_month
        , block_date
        , block_time
        , inner_instruction_index
        , outer_instruction_index
        , outer_executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , pool_id
        , token_0_vault
        , token_1_vault
        , surrogate_key
    FROM {{ ref('jupiter_lend_solana_stg_decoded_swaps') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_date') }}
        {% else %}
        AND block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

-- Narrow the transfer scan to the block slots that actually contain a swap
, swap_slots AS (
    SELECT DISTINCT block_date, block_slot
    FROM swaps
)

, transfers_pruned AS (
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
        {% if is_incremental() %}
        AND {{ incremental_predicate('tf.block_date') }}
        {% else %}
        AND tf.block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

, matched AS (
    SELECT
          s.block_month
        , s.block_date
        , s.block_time
        , s.block_slot
        , CASE WHEN s.is_inner = false THEN 'direct' ELSE s.outer_executing_account END AS trade_source
        , max(CASE WHEN tf.from_token_account IN (s.token_0_vault, s.token_1_vault) THEN tf.amount END)             AS token_bought_amount_raw
        , max(CASE WHEN tf.to_token_account   IN (s.token_0_vault, s.token_1_vault) THEN tf.amount END)             AS token_sold_amount_raw
        , max(CASE WHEN tf.from_token_account IN (s.token_0_vault, s.token_1_vault) THEN tf.from_token_account END) AS token_bought_vault
        , max(CASE WHEN tf.to_token_account   IN (s.token_0_vault, s.token_1_vault) THEN tf.to_token_account END)   AS token_sold_vault
        , max(CASE WHEN tf.from_token_account IN (s.token_0_vault, s.token_1_vault) THEN tf.token_mint_address END) AS token_bought_mint_address
        , max(CASE WHEN tf.to_token_account   IN (s.token_0_vault, s.token_1_vault) THEN tf.token_mint_address END) AS token_sold_mint_address
        , s.pool_id AS project_program_id
        , s.tx_signer AS trader_id
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
        , s.surrogate_key
    FROM swaps s
    INNER JOIN transfers_pruned tf
        ON  tf.tx_id = s.tx_id
        AND tf.block_date = s.block_date
        AND tf.block_slot = s.block_slot
        AND tf.outer_instruction_index = s.outer_instruction_index
        AND tf.inner_instruction_index BETWEEN s.inner_instruction_index + 2 AND s.inner_instruction_index + 6
        AND (
                (tf.inner_instruction_index IN (s.inner_instruction_index + 2, s.inner_instruction_index + 3)
                 AND tf.to_token_account   IN (s.token_0_vault, s.token_1_vault))
             OR (tf.inner_instruction_index IN (s.inner_instruction_index + 5, s.inner_instruction_index + 6)
                 AND tf.from_token_account IN (s.token_0_vault, s.token_1_vault))
            )
    GROUP BY
          s.block_month
        , s.block_date
        , s.block_time
        , s.block_slot
        , CASE WHEN s.is_inner = false THEN 'direct' ELSE s.outer_executing_account END
        , s.pool_id
        , s.tx_signer
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
        , s.surrogate_key
    HAVING 1=1
        AND count_if(tf.to_token_account   IN (s.token_0_vault, s.token_1_vault)) = 1
        AND count_if(tf.from_token_account IN (s.token_0_vault, s.token_1_vault)) = 1
)

SELECT
      'solana' AS blockchain
    , 'jupiter_lend' AS project
    , 1 AS version
    , 'dex' AS version_name
    , block_month
    , block_date
    , block_time
    , block_slot
    , trade_source
    , token_bought_amount_raw
    , token_sold_amount_raw
    , CAST(NULL AS DOUBLE) AS fee_tier
    , token_bought_mint_address
    , token_sold_mint_address
    , token_bought_vault
    , token_sold_vault
    , project_program_id
    , '{{ program_id }}' AS project_main_id
    , trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
    , surrogate_key
FROM matched
