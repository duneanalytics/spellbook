{{ config(
	schema = 'unidex_v1_optimism',
	alias = 'perpetual_trades',
	partition_by = ['block_month'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
	)
}}

{% set project_start_date = '2022-10-24' %}

WITH positions AS (
	SELECT
		user AS user
		,productId
		,CAST(isLong AS VARCHAR) AS isLong
		,margin 
        ,size
		,0 AS fee
		,contract_address
		,evt_tx_hash
		,evt_index
		,evt_block_time
		,evt_block_number
		,'1' AS version
	FROM {{ source('unidex_optimism', 'trading_evt_NewOrder') }}
	{% if is_incremental() %}
	WHERE {{ incremental_predicate('evt_block_time') }}
	{% endif %}

	UNION ALL
	--closing positions
	SELECT
		user
		,productId
		,'close' AS action
		,margin
        ,size
		,0 AS fee
		,contract_address
		,evt_tx_hash
		,evt_index
		,evt_block_time
		,evt_block_number
		,'1' AS version
	FROM {{ source('unidex_optimism', 'trading_evt_ClosePosition') }}
	{% if is_incremental() %}
	WHERE {{ incremental_predicate('evt_block_time') }}
	{% endif %}
),

perps AS (
	SELECT
		evt_block_time AS block_time
		,evt_block_number AS block_number
		,CAST(NULL as VARCHAR) AS virtual_asset
        ,CAST(NULL as VARCHAR) AS underlying_asset
		,CAST(NULL as VARCHAR) AS market
		,contract_address AS market_address
		,(size/1e8) AS volume_usd
		,fee/1e8 AS fee_usd
		,margin/1e8 AS margin_usd
		,CASE
		WHEN isLong = 'true' THEN 'long'
		WHEN isLong = 'false' THEN 'short'
		ELSE CAST(isLong as VARCHAR)
		END AS trade
		,'Unidex' AS project
		,version
		,'Unidex' AS frontend
		,user AS trader
		,size AS volume_raw
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
	,CAST(perps.volume_raw as UINT256) as volume_raw
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
	{% else %}
	AND {{ incremental_predicate('tx.block_time') }}
	{% endif %}