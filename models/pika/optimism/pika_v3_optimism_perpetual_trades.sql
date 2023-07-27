{{ config(
	tags=['legacy'],
	
	schema = 'pika_v3_optimism',
	alias = alias('perpetual_trades', legacy_model=True),
	partition_by = ['block_date'],
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
		,CAST(isLong AS VARCHAR(5)) AS isLong
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
		,'3' AS version
	FROM {{ source('pika_perp_v3_optimism', 'PikaPerpV3_evt_NewPosition') }}
	{% if is_incremental() %}
	WHERE evt_block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
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
		,'3' AS version
	FROM {{ source('pika_perp_v3_optimism', 'PikaPerpV3_evt_ClosePosition') }}
	{% if is_incremental() %}
	WHERE evt_block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
	{% endif %}
),

perps AS (
	SELECT
		evt_block_time AS block_time
		,evt_block_number AS block_number
		
		,CASE
		WHEN productId = 1 OR productId = 16 THEN 'ETH'
		WHEN productId = 2 OR productId = 17 THEN 'BTC'
		WHEN productId = 3 OR productId = 18 THEN 'LINK'
		WHEN productId = 4 OR productId = 19 THEN 'SNX'
		WHEN productId = 5 OR productId = 20 THEN 'SOL'
		WHEN productId = 6 OR productId = 21 THEN 'AVAX'
		WHEN productId = 7 OR productId = 22  THEN 'MATIC'
		WHEN productId = 8 THEN 'LUNA'
		WHEN productId = 9 OR productId = 23 THEN 'AAVE'
		WHEN productId = 10 OR productId = 24 THEN 'APE'
		WHEN productId = 11 OR productId = 25 THEN 'AXS'
		WHEN productId = 12 OR productId = 26 THEN 'UNI'
		ELSE CONCAT ('product_id_', productId)
		END AS virtual_asset

		,CASE
		WHEN productId = 1 OR productId = 16 THEN 'ETH'
		WHEN productId = 2 OR productId = 17 THEN 'BTC'
		WHEN productId = 3 OR productId = 18 THEN 'LINK'
		WHEN productId = 4 OR productId = 19 THEN 'SNX'
		WHEN productId = 5 OR productId = 20 THEN 'SOL'
		WHEN productId = 6 OR productId = 21 THEN 'AVAX'
		WHEN productId = 7 OR productId = 22  THEN 'MATIC'
		WHEN productId = 8 THEN 'LUNA'
		WHEN productId = 9 OR productId = 23 THEN 'AAVE'
		WHEN productId = 10 OR productId = 24 THEN 'APE'
		WHEN productId = 11 OR productId = 25 THEN 'AXS'
		WHEN productId = 12 OR productId = 26 THEN 'UNI'
		ELSE CONCAT ('product_id_', productId)
		END AS underlying_asset

		,CASE
		WHEN productId = 1 OR productId = 16 THEN 'ETH-USD'
		WHEN productId = 2 OR productId = 17 THEN 'BTC-USD'
		WHEN productId = 3 OR productId = 18 THEN 'LINK-USD'
		WHEN productId = 4 OR productId = 19 THEN 'SNX-USD'
		WHEN productId = 5 OR productId = 20 THEN 'SOL-USD'
		WHEN productId = 6 OR productId = 21 THEN 'AVAX-USD'
		WHEN productId = 7 OR productId = 22  THEN 'MATIC-USD'
		WHEN productId = 8 THEN 'LUNA-USD'
		WHEN productId = 9 OR productId = 23 THEN 'AAVE-USD'
		WHEN productId = 10 OR productId = 24 THEN 'APE-USD'
		WHEN productId = 11 OR productId = 25 THEN 'AXS-USD'
		WHEN productId = 12 OR productId = 26 THEN 'UNI-USD'
		ELSE CONCAT ('product_id_', productId)
		END AS market
		
		,contract_address AS market_address
		,(margin/1e8) * (leverage/1e8) AS volume_usd
		,fee/1e8 AS fee_usd
		,margin/1e8 AS margin_usd

		,CASE
		WHEN isLong = 'true' THEN 'long'
		WHEN isLong = 'false' THEN 'short'
		ELSE isLong
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
	,TRY_CAST(date_trunc('DAY', perps.block_time) AS date) AS block_date
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
	,tx.from AS tx_from
	,tx.to AS tx_to
	,perps.evt_index
FROM perps
INNER JOIN {{ source('optimism', 'transactions') }} AS tx
	ON perps.tx_hash = tx.hash
	AND perps.block_number = tx.block_number
	{% if not is_incremental() %}
	AND tx.block_time >= '{{project_start_date}}'
	{% endif %}
	{% if is_incremental() %}
	AND tx.block_time >= DATE_TRUNC("DAY", NOW () - INTERVAL '1 WEEK')
	{% endif %}
