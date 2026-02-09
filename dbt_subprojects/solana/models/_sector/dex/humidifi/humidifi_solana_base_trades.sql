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
    , pre_hook = '{{ enforce_join_distribution("PARTITIONED") }}'
  )
}}

{% set project_start_date = '2025-06-13' %}

WITH swaps AS (
	SELECT
    block_slot
    , block_month
    , block_date
    , block_time
    , inner_instruction_index
    , outer_instruction_index
    , inner_executing_account
    , outer_executing_account
    , executing_account
    , is_inner
    , tx_id
    , tx_signer
    , tx_index
    , pool_id
    , vault_a
    , vault_b
    , surrogate_key
	FROM {{ ref('humidifi_solana_stg_raw_swaps') }}
	WHERE
		1=1
		{% if is_incremental() -%}
		AND {{ incremental_predicate('block_date') }}
		{% else -%}
		AND block_date >= DATE '{{ project_start_date }}'
		{% endif -%}
)

-- Step 1: compute expected transfer positions for semi-join pruning
, swap_transfer_keys AS (
	SELECT DISTINCT
		  tx_id
		, block_date
		, block_slot
		, outer_instruction_index
		, transfer_inner_instruction_index
	FROM (
		SELECT
			  tx_id
			, block_date
			, block_slot
			, outer_instruction_index
			, inner_instruction_index + 1 AS transfer_inner_instruction_index
		FROM swaps

		UNION ALL

		SELECT
			  tx_id
			, block_date
			, block_slot
			, outer_instruction_index
			, inner_instruction_index + 2 AS transfer_inner_instruction_index
		FROM swaps

		UNION ALL

		SELECT
			  tx_id
			, block_date
			, block_slot
			, outer_instruction_index
			, inner_instruction_index + 3 AS transfer_inner_instruction_index
		FROM swaps
	)
)

-- Step 2: filter transfers using a SEMI join (EXISTS) so the hash build is on swap_transfer_keys, not transfers
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
	WHERE
		1=1
		AND tf.token_version IN ('spl_token', 'spl_token_2022')
		{% if is_incremental() -%}
		AND {{ incremental_predicate('tf.block_date') }}
		{% else -%}
		AND tf.block_date >= DATE '{{ project_start_date }}'
		{% endif -%}
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

-- Step 3: join pruned transfers with swaps and determine buy/sell via vault matching
, transfers_raw AS (
	SELECT
		  s.block_slot
		, s.block_month
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
		, s.vault_a
		, s.vault_b
		, s.surrogate_key
		, tp.inner_instruction_index AS tf_inner_index
		, tp.amount
		, tp.from_token_account
		, tp.to_token_account
		, tp.token_mint_address
		, CASE
			WHEN tp.from_token_account IN (s.vault_a, s.vault_b) THEN 'buy'
			WHEN tp.to_token_account IN (s.vault_a, s.vault_b) THEN 'sell'
		  END AS transfer_type
	FROM swaps s
	INNER JOIN transfers_pruned tp
		ON  tp.tx_id = s.tx_id
		AND tp.block_date = s.block_date
		AND tp.block_slot = s.block_slot
		AND tp.outer_instruction_index = s.outer_instruction_index
		AND tp.inner_instruction_index IN (s.inner_instruction_index + 1, s.inner_instruction_index + 2, s.inner_instruction_index + 3)
	WHERE (
		tp.from_token_account IN (s.vault_a, s.vault_b)
		OR tp.to_token_account IN (s.vault_a, s.vault_b)
	)
)

-- Step 4: aggregate to one row per swap
, transfers AS (
	SELECT
		  block_date
		, block_time
		, block_slot
		, block_month
		, CASE WHEN is_inner = false THEN 'direct' ELSE outer_executing_account END AS trade_source
		, MAX(CASE WHEN transfer_type = 'buy' THEN amount END) AS token_bought_amount_raw
		, MAX(CASE WHEN transfer_type = 'sell' THEN amount END) AS token_sold_amount_raw
		, MAX(CASE WHEN transfer_type = 'buy' THEN from_token_account END) AS token_bought_vault
		, MAX(CASE WHEN transfer_type = 'sell' THEN to_token_account END) AS token_sold_vault
		, MAX(CASE WHEN transfer_type = 'buy' THEN token_mint_address END) AS token_bought_mint_address
		, MAX(CASE WHEN transfer_type = 'sell' THEN token_mint_address END) AS token_sold_mint_address
		, pool_id
		, tx_signer AS trader_id
		, tx_id
		, outer_instruction_index
		, inner_instruction_index
		, tx_index
		, surrogate_key
	FROM transfers_raw
	GROUP BY
		  block_date
		, block_time
		, block_slot
		, block_month
		, CASE WHEN is_inner = false THEN 'direct' ELSE outer_executing_account END
		, pool_id
		, tx_signer
		, tx_id
		, outer_instruction_index
		, inner_instruction_index
		, tx_index
		, surrogate_key
	HAVING COUNT_IF(transfer_type = 'buy') BETWEEN 1 AND 2
	   AND COUNT_IF(transfer_type = 'sell') BETWEEN 1 AND 2
)

SELECT
	  'solana' AS blockchain
	, 'humidifi' AS project
	, 1 AS version
	, 'v1' AS version_name
	, block_month
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
	, pool_id AS project_program_id
	, '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp' AS project_main_id
	, trader_id
	, tx_id
	, outer_instruction_index
	, inner_instruction_index
	, tx_index
	, surrogate_key
FROM transfers
