{{ config(
	tags=['dunesql'],
	schema = 'pika_v2_optimism',
	alias = alias('perpetual_trades'),
	partition_by = ['block_month'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "pika",
                                \'["msilb7", "drethereum", "rplust"]\') }}'
	)
}}

{% set project_start_date = '2021-11-22' %}

WITH positions AS (
	SELECT
		positionId
		,user AS user
		,productId
		,CAST(isLong AS VARCHAR) AS isLong
		,price
		,oraclePrice
		,margin
		,leverage
		,fee
		,contract_address
		,evt_tx_hash
		,evt_index
		,evt_block_time
		,evt_block_number
		,'2' AS version
	FROM {{ source('pika_perp_v2_optimism', 'PikaPerpV2_evt_NewPosition') }}
	{% if is_incremental() %}
	WHERE evt_block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '7' Day)
	{% endif %}

	UNION ALL
	--closing positions
	SELECT
		positionId
		,user
		,productId
		,'close' AS action
		,price
		,entryPrice
		,margin
		,leverage
		,fee
		,contract_address
		,evt_tx_hash
		,evt_index
		,evt_block_time
		,evt_block_number
		,'2' AS version
	FROM {{ source('pika_perp_v2_optimism', 'PikaPerpV2_evt_ClosePosition') }}
	{% if is_incremental() %}
	WHERE evt_block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '7' Day)
	{% endif %}
),

perps AS (
	SELECT
		evt_block_time AS block_time
		,evt_block_number AS block_number
		
		,CASE
		WHEN productId = UINT256 '1' OR productId = UINT256 '16' THEN 'ETH'
		WHEN productId = UINT256 '2' OR productId = UINT256 '17' THEN 'BTC'
		WHEN productId = UINT256 '3' OR productId = UINT256 '18' THEN 'LINK'
		WHEN productId = UINT256 '4' OR productId = UINT256 '19' THEN 'SNX'
		WHEN productId = UINT256 '5' OR productId = UINT256 '20' THEN 'SOL'
		WHEN productId = UINT256 '6' OR productId = UINT256 '21' THEN 'AVAX'
		WHEN productId = UINT256 '7' OR productId = UINT256 '22'  THEN 'MATIC'
		WHEN productId = UINT256 '8' THEN 'LUNA'
		WHEN productId = UINT256 '9' OR productId = UINT256 '23' THEN 'AAVE'
		WHEN productId = UINT256 '10' OR productId = UINT256 '24' THEN 'APE'
		WHEN productId = UINT256 '11' OR productId = UINT256 '25' THEN 'AXS'
		WHEN productId = UINT256 '12' OR productId = UINT256 '26' THEN 'UNI'
		ELSE CONCAT ('product_id_', CAST(productId as VARCHAR))
		END AS virtual_asset

		,CASE
		WHEN productId = UINT256 '1' OR productId = UINT256 '16' THEN 'ETH'
		WHEN productId = UINT256 '2' OR productId = UINT256 '17' THEN 'BTC'
		WHEN productId = UINT256 '3' OR productId = UINT256 '18' THEN 'LINK'
		WHEN productId = UINT256 '4' OR productId = UINT256 '19' THEN 'SNX'
		WHEN productId = UINT256 '5' OR productId = UINT256 '20' THEN 'SOL'
		WHEN productId = UINT256 '6' OR productId = UINT256 '21' THEN 'AVAX'
		WHEN productId = UINT256 '7' OR productId = UINT256 '22'  THEN 'MATIC'
		WHEN productId = UINT256 '8' THEN 'LUNA'
		WHEN productId = UINT256 '9' OR productId = UINT256 '23' THEN 'AAVE'
		WHEN productId = UINT256 '10' OR productId = UINT256 '24' THEN 'APE'
		WHEN productId = UINT256 '11' OR productId = UINT256 '25' THEN 'AXS'
		WHEN productId = UINT256 '12' OR productId = UINT256 '26' THEN 'UNI'
		ELSE CONCAT ('product_id_', CAST(productId as VARCHAR))
		END AS underlying_asset

		,CASE
		WHEN productId = UINT256 '1' OR productId = UINT256 '16' THEN 'ETH'
		WHEN productId = UINT256 '2' OR productId = UINT256 '17' THEN 'BTC'
		WHEN productId = UINT256 '3' OR productId = UINT256 '18' THEN 'LINK'
		WHEN productId = UINT256 '4' OR productId = UINT256 '19' THEN 'SNX'
		WHEN productId = UINT256 '5' OR productId = UINT256 '20' THEN 'SOL'
		WHEN productId = UINT256 '6' OR productId = UINT256 '21' THEN 'AVAX'
		WHEN productId = UINT256 '7' OR productId = UINT256 '22'  THEN 'MATIC'
		WHEN productId = UINT256 '8' THEN 'LUNA-USD'
		WHEN productId = UINT256 '9' OR productId = UINT256 '23' THEN 'AAVE'
		WHEN productId = UINT256 '10' OR productId = UINT256 '24' THEN 'APE'
		WHEN productId = UINT256 '11' OR productId = UINT256 '25' THEN 'AXS'
		WHEN productId = UINT256 '12' OR productId = UINT256 '26' THEN 'UNI'
		ELSE CONCAT ('product_id_', CAST(productId as VARCHAR))
		END AS market
		
		,contract_address AS market_address
		,(margin/1e8) * (leverage/1e8) AS volume_usd
		,fee/1e8 AS fee_usd
		,margin/1e8 AS margin_usd

		,CASE
		WHEN isLong = 'true' THEN 'long'
		WHEN isLong = 'false' THEN 'short'
		ELSE CAST(isLong as VARCHAR)
		END AS trade

		,'Pika' AS project
		,version
		,'Pika' AS frontend
		,user AS trader
		,margin * leverage AS volume_raw
		,evt_tx_hash AS tx_hash
		,evt_index
	FROM positions

)

SELECT
	'optimism' AS blockchain
	,CAST(date_trunc('DAY', perps.block_time) AS date) AS block_date
	,CAST(date_trunc('MONTH', perps.block_time) AS date) AS block_month
	,perps.block_time
	,perps.virtual_asset
	,perps.underlying_asset
	,perps.market
	,perps.market_address
	,perps.volume_usd
	,perps.fee_usd
	,perps.margin_usd
	,perps.trade
	,perps.project
	,perps.version
	,perps.frontend
	,perps.trader
	,perps.volume_raw
	,perps.tx_hash
	,tx."from" AS tx_from
	,tx."to" AS tx_to
	,perps.evt_index
FROM perps
INNER JOIN {{ source('optimism', 'transactions') }} AS tx
	ON perps.tx_hash = tx.hash
	AND perps.block_number = tx.block_number
	{% if not is_incremental() %}
	AND tx.block_time >= DATE '{{project_start_date}}'
	{% endif %}
	{% if is_incremental() %}
	AND tx.block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '7' Day)
	{% endif %}
