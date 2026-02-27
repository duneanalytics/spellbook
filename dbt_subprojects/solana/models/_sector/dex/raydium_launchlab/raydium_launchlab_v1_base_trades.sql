{{
  config(
    schema = 'raydium_launchlab_v1'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_month', 'block_slot', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']
    , pre_hook='{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-03-17' %}

WITH swaps AS (
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
        , pool_id
        , base_token_mint
        , quote_token_mint
        , base_vault
        , quote_vault
        , is_buy
        , platform_config
        , platform_name
        , platform_params
    FROM {{ ref('raydium_launchlab_v1_stg_swaps') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_date') }}
        {% else %}
        AND block_date >= DATE '{{ project_start_date }}'
        AND block_date < DATE '2025-03-24'
        {% endif %}
)

, swap_transfer_keys AS (
    SELECT DISTINCT
          tx_id
        , block_date
        , block_slot
        , outer_instruction_index
        , inner_instruction_index AS swap_inner_instruction_index
        , transfer_inner_instruction_index
    FROM (
        SELECT
              tx_id
            , block_date
            , block_slot
            , outer_instruction_index
            , inner_instruction_index
            , inner_instruction_index + 1 AS transfer_inner_instruction_index
        FROM swaps

        UNION ALL

        SELECT
              tx_id
            , block_date
            , block_slot
            , outer_instruction_index
            , inner_instruction_index
            , inner_instruction_index + 2 AS transfer_inner_instruction_index
        FROM swaps

        UNION ALL

        SELECT
              tx_id
            , block_date
            , block_slot
            , outer_instruction_index
            , inner_instruction_index
            , inner_instruction_index + 3 AS transfer_inner_instruction_index
        FROM swaps
    )
)

, swap_slots AS (
    SELECT DISTINCT block_date, block_slot
    FROM swap_transfer_keys
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
        {% if is_incremental() %}
        AND {{ incremental_predicate('tf.block_date') }}
        {% else %}
        AND tf.block_date >= DATE '{{ project_start_date }}'
        AND tf.block_date < DATE '2025-03-24'
        {% endif %}
        AND EXISTS (
            SELECT 1
            FROM swap_transfer_keys sk
            WHERE
                sk.tx_id = tf.tx_id
                AND sk.block_date = tf.block_date
                AND sk.block_slot = tf.block_slot
                AND sk.outer_instruction_index = tf.outer_instruction_index
                AND sk.transfer_inner_instruction_index = tf.inner_instruction_index
        )
)

, transfers_filtered AS (
    SELECT
          s.tx_id
        , s.block_date
        , s.block_slot
        , s.outer_instruction_index
        , s.inner_instruction_index AS swap_inner_instruction_index
        , tp.inner_instruction_index
        , tp.amount
        , tp.from_token_account
        , tp.to_token_account
        , tp.token_mint_address
        , CASE
            WHEN tp.token_mint_address = s.base_token_mint
                 AND ((s.is_buy = 1 AND tp.from_token_account = s.base_vault)
                   OR (s.is_buy = 0 AND tp.to_token_account = s.base_vault))
            THEN 'base'
            WHEN tp.token_mint_address = s.quote_token_mint
                 AND ((s.is_buy = 1 AND tp.to_token_account = s.quote_vault)
                   OR (s.is_buy = 0 AND tp.from_token_account = s.quote_vault))
                 AND (s.is_inner = false OR tp.inner_instruction_index >= s.inner_instruction_index + 2)
            THEN 'quote'
          END AS transfer_side
    FROM swaps s
    INNER JOIN transfers_pruned tp
        ON  tp.tx_id = s.tx_id
        AND tp.block_date = s.block_date
        AND tp.block_slot = s.block_slot
        AND tp.outer_instruction_index = s.outer_instruction_index
        AND tp.inner_instruction_index BETWEEN s.inner_instruction_index + 1 AND s.inner_instruction_index + 3
)

, transfers AS (
    SELECT
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE
            WHEN s.is_inner = false THEN 'direct'
            ELSE s.outer_executing_account
          END AS trade_source
        , s.is_buy
        , max(CASE WHEN tf.transfer_side = 'base' THEN tf.amount END) AS base_amount
        , max(CASE WHEN tf.transfer_side = 'quote' THEN tf.amount END) AS quote_amount
        , max(CASE WHEN tf.transfer_side = 'base' THEN tf.from_token_account END) AS base_from_token_account
        , max(CASE WHEN tf.transfer_side = 'base' THEN tf.to_token_account END) AS base_to_token_account
        , max(CASE WHEN tf.transfer_side = 'quote' THEN tf.from_token_account END) AS quote_from_token_account
        , max(CASE WHEN tf.transfer_side = 'quote' THEN tf.to_token_account END) AS quote_to_token_account
        , s.base_token_mint
        , s.quote_token_mint
        , s.pool_id
        , s.tx_signer AS trader_id
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
        , s.platform_config
        , s.platform_name
        , s.platform_params
    FROM swaps s
    INNER JOIN transfers_filtered tf
        ON  tf.tx_id = s.tx_id
        AND tf.block_date = s.block_date
        AND tf.block_slot = s.block_slot
        AND tf.outer_instruction_index = s.outer_instruction_index
        AND tf.swap_inner_instruction_index = s.inner_instruction_index
        AND tf.transfer_side IS NOT NULL
    GROUP BY
          s.block_date
        , s.block_time
        , s.block_slot
        , CASE
            WHEN s.is_inner = false THEN 'direct'
            ELSE s.outer_executing_account
          END
        , s.is_buy
        , s.base_token_mint
        , s.quote_token_mint
        , s.pool_id
        , s.tx_signer
        , s.tx_id
        , s.outer_instruction_index
        , s.inner_instruction_index
        , s.tx_index
        , s.platform_config
        , s.platform_name
        , s.platform_params
    HAVING 1=1
        AND count_if(tf.transfer_side = 'base') = 1
        AND count_if(tf.transfer_side = 'quote') = 1
)

SELECT
      'solana' AS blockchain
    , 'raydium_launchlab' AS project
    , 1 AS version
    , CAST(date_trunc('month', block_time) AS DATE) AS block_month
    , block_time
    , block_slot
    , trade_source
    , CASE WHEN is_buy = 1 THEN base_amount ELSE quote_amount END AS token_bought_amount_raw
    , CASE WHEN is_buy = 0 THEN base_amount ELSE quote_amount END AS token_sold_amount_raw
    , CAST(NULL AS DOUBLE) AS fee_tier
    , CASE WHEN is_buy = 0 THEN base_token_mint ELSE quote_token_mint END AS token_sold_mint_address
    , CASE WHEN is_buy = 1 THEN base_token_mint ELSE quote_token_mint END AS token_bought_mint_address
    , CASE WHEN is_buy = 0 THEN base_to_token_account ELSE quote_to_token_account END AS token_sold_vault
    , CASE WHEN is_buy = 1 THEN base_from_token_account ELSE quote_from_token_account END AS token_bought_vault
    , pool_id AS project_program_id
    , 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj' AS project_main_id
    , trader_id
    , tx_id
    , outer_instruction_index
    , inner_instruction_index
    , tx_index
    , platform_config AS account_platform_config
    , platform_name
    , platform_params
FROM transfers
