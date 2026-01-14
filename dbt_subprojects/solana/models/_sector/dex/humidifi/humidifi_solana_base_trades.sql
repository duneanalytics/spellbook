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

{% set project_start_date = '2026-01-01' %}

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

, transfers AS (
	SELECT
		  s.block_date
		, s.block_time
		, s.block_slot
		, CASE
			WHEN s.is_inner = false THEN 'direct'
			ELSE s.outer_executing_account
		  END AS trade_source
		, t_buy.amount AS token_bought_amount_raw
		, t_sell.amount AS token_sold_amount_raw
		, t_buy.from_token_account AS token_bought_vault
		, t_sell.to_token_account AS token_sold_vault
		, t_buy.token_mint_address AS token_bought_mint_address
		, t_sell.token_mint_address AS token_sold_mint_address
		, s.pool_id AS project_program_id
		, s.tx_signer AS trader_id
		, s.tx_id
		, s.outer_instruction_index
		, s.inner_instruction_index
		, s.tx_index
		, s.surrogate_key
	FROM swaps s

	-- sold (inner_instruction_index + 1)
	INNER JOIN {{ source('tokens_solana', 'transfers') }} t_sell
		ON  t_sell.tx_id = s.tx_id
		AND t_sell.block_date = s.block_date
		AND t_sell.block_slot = s.block_slot
		AND t_sell.outer_instruction_index = s.outer_instruction_index
		AND t_sell.inner_instruction_index = s.inner_instruction_index + 1
		AND t_sell.token_version IN ('spl_token', 'spl_token_2022')
		{% if is_incremental() -%}
		AND {{ incremental_predicate('t_sell.block_date') }}
		{% else -%}
		AND t_sell.block_date >= DATE '{{ project_start_date }}'
		{% endif -%}

	-- bought (inner_instruction_index + 2)
	INNER JOIN {{ source('tokens_solana', 'transfers') }} t_buy
		ON  t_buy.tx_id = s.tx_id
		AND t_buy.block_date = s.block_date
		AND t_buy.block_slot = s.block_slot
		AND t_buy.outer_instruction_index = s.outer_instruction_index
		AND t_buy.inner_instruction_index = s.inner_instruction_index + 2
		AND t_buy.token_version IN ('spl_token', 'spl_token_2022')
		{% if is_incremental() -%}
		AND {{ incremental_predicate('t_buy.block_date') }}
		{% else -%}
		AND t_buy.block_date >= DATE '{{ project_start_date }}'
		{% endif -%}
)

SELECT
	  'solana' AS blockchain
	, 'humidifi' AS project
	, 1 AS version
	, 'v1' AS version_name
	, cast(date_trunc('month', s.block_date) AS DATE) AS block_month
	, s.block_time
	, s.block_slot
	, s.block_date
	, s.trade_source
	, s.token_bought_amount_raw
	, s.token_sold_amount_raw
	, CAST(NULL AS DOUBLE) AS fee_tier
	, s.token_bought_mint_address
	, s.token_sold_mint_address
	, s.token_bought_vault
	, s.token_sold_vault
	, s.project_program_id
	, '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp' AS project_main_id
	, s.trader_id
	, s.tx_id
	, s.outer_instruction_index
	, s.inner_instruction_index
	, s.tx_index
	, s.surrogate_key
FROM transfers s
