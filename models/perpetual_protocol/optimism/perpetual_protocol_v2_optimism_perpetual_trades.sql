{{ config(
	
	schema = 'perpetual_protocol_v2_optimism',
	alias = 'perpetual_trades',
	partition_by = ['block_month'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "perpetual_protocol",
                                \'["msilb7", "drethereum", "rplust"]\') }}'
	)
}}

{% set project_start_date = '2021-11-22' %}

WITH perps AS (
	SELECT
		p.evt_block_time AS block_time
		,p.evt_block_number AS block_number
		,p.baseToken
		,pp.pool AS market_address
		,ABS(p.exchangedPositionNotional)/1e18 AS volume_usd
		,p.fee/1e18 AS fee_usd
		,MAX(co.output_0)/1e6 AS margin_usd

		,CASE
		WHEN CAST(p.exchangedPositionSize AS DOUBLE) > 0 THEN 'long'
		WHEN CAST(p.exchangedPositionSize AS DOUBLE) < 0 THEN 'short'
		ELSE 'NA'
		END AS trade

		,'Perpetual' AS project
		,'2' AS version
		,'Perpetual' AS frontend
		,p.trader
		,CAST(ABS(exchangedPositionNotional) as UINT256) as volume_raw
		,p.evt_tx_hash AS tx_hash
		,p.evt_index
	FROM {{ source('perp_v2_optimism', 'ClearingHouse_evt_PositionChanged') }} AS p
	LEFT JOIN {{ source('perp_v2_optimism', 'Vault_call_getFreeCollateralByRatio') }} AS co
		ON p.evt_tx_hash = co.call_tx_hash
		AND co.call_success = true
		{% if is_incremental() %}
		AND co.call_block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '7' Day)
		{% endif %}
	LEFT JOIN {{ source('perp_v2_optimism', 'MarketRegistry_evt_PoolAdded') }} AS pp
		ON p.baseToken = pp.baseToken
	{% if is_incremental() %}
	WHERE p.evt_block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '7' Day)
	{% endif %}
	GROUP BY 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15
)

SELECT
	'optimism' AS blockchain
	,CAST(date_trunc('DAY', perps.block_time) AS date) AS block_date
	,CAST(date_trunc('MONTH', perps.block_time) AS date) AS block_month
	,perps.block_time
	,COALESCE(e.symbol, CAST(perps.baseToken AS VARCHAR)) AS virtual_asset
	,SUBSTRING(e.symbol, 2) AS underlying_asset
	,CONCAT(e.symbol, '-', 'USD') AS market
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
LEFT JOIN {{ source('tokens', 'erc20') }} AS e
	ON perps.baseToken = e.contract_address
	AND e.blockchain = 'optimism'
INNER JOIN {{ source('optimism', 'transactions') }} AS tx
	ON perps.tx_hash = tx.hash
	AND perps.block_number = tx.block_number
	{% if not is_incremental() %}
	AND tx.block_time >= DATE '{{project_start_date}}'
	{% endif %}
	{% if is_incremental() %}
	AND tx.block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '7' Day)
	{% endif %}
