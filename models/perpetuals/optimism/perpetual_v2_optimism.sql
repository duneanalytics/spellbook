{{ config(
	schema = 'perpetual_v2_optimism',
	partition_by = ['block_time'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_time', 'trade_id']
	)
}}

WITH perps AS (
	SELECT
		p.evt_block_time AS block_time
		,p.baseToken
		,pp.pool AS market_address
		,ABS(p.exchangedPositionNotional)/1e18 AS volume_usd
		,p.fee/1e18 AS fee_usd
		,co.output_0/1e6 AS margin_usd

		,CASE
		WHEN CAST(p.exchangedPositionSize AS DOUBLE) > 0 THEN 'long'
		WHEN CAST(p.exchangedPositionSize AS DOUBLE) < 0 THEN 'short'
		ELSE 'NA'
		END AS trade

		,'Perpetual' AS project
		,'2' AS version
		,p.trader
		,p.exchangedPositionNotional AS volume_raw
		,p.evt_tx_hash AS tx_hash
		,p.evt_index
	FROM {{ source('perp_v2_optimism', 'ClearingHouse_evt_PositionChanged') }} AS p
	LEFT JOIN {{ source('perp_v2_optimism', 'Vault_call_getFreeCollateralByRatio') }} AS co
		ON p.evt_tx_hash = co.call_tx_hash
	{% if is_incremental() %}
	WHERE p.evt_block_time >= (SELECT MAX(block_time) FROM {{ this }})
	{% endif %}
	LEFT JOIN {{ source('perp_v2_optimism', 'MarketRegistry_evt_PoolAdded') }} AS pp
		ON p.baseToken = pp.baseToken
	WHERE co.call_success = true
)

SELECT
	'optimism' AS blockchain
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
	,perps.trader
	,perps.volume_raw
	,perps.tx_hash
	,tx.from AS tx_from
	,tx.to AS tx_to
	,perps.evt_index
	,perps.project || perps.version || perps.tx_hash || perps.evt_index AS trade_id
FROM perps
LEFT JOIN {{ ref('tokens_optimism_erc20') }} AS e
	ON perps.baseToken = e.contract_address
INNER JOIN {{ source('optimism', 'transactions') }} AS tx
	ON perps.tx_hash = tx.hash
	{% if not is_incremental() %}
	AND tx.block_time >= (SELECT MIN(block_time) FROM perps)
	{% endif %}
	{% if is_incremental() %}
	AND TRY_CAST(DATE_TRUNC('DAY', tx.block_time) AS date) = TRY_CAST(date_trunc('DAY', perps.block_time) AS date)
	{% endif %}
