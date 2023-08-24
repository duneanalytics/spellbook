{{ config(
	tags=['dunesql'],
    schema = 'synthetix_v2_optimism',
	alias = alias('perpetual_trades'),
	partition_by = ['block_month'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "synthetix",
                                \'["rplust"]\') }}'
	)
}}

{% set project_start_date = '2021-11-22' %}

WITH 

asset_price AS (
	SELECT
		s.contract_address AS market_address
		,from_utf8(sm.asset) as asset
		,s.evt_block_time
		,AVG(s.lastPrice/1e18) AS price
	FROM {{ source('synthetix_futuresmarket_optimism', 'ProxyPerpsV2_evt_PositionModified') }} s
	LEFT JOIN {{ source('synthetix_optimism', 'FuturesMarketManager_evt_MarketAdded') }} sm
		ON s.contract_address = sm.market
	{% if is_incremental() %}
	WHERE s.evt_block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '7' Day)
	{% endif %}
	GROUP BY 1, 2 , 3 
),

synthetix_markets AS (
	SELECT DISTINCT
		from_utf8(asset) as asset		
		,market
		,from_utf8(marketKey) as marketKey
	FROM {{ source('synthetix_optimism', 'FuturesMarketManager_evt_MarketAdded') }}
),

perps AS (
	SELECT
		s.evt_block_time AS block_time
		,s.evt_block_number AS block_number
		,sm.asset as virtual_asset		
		,CASE 
			WHEN SUBSTRING((sm.asset), 1, 1) = 's' THEN SUBSTRING((sm.asset), 2)
			ELSE sm.asset
		END as underlying_asset
		,sm.marketKey as market
		,s.contract_address AS market_address
		,ABS(s.tradeSize)/1e18 * p.price AS volume_usd
		,s.fee/1e18 AS fee_usd
		,s.margin/1e18 AS margin_usd
		,(ABS(s.tradeSize)/1e18 * p.price) / (s.margin/1e18) AS leverage_ratio
		,CASE
		WHEN (CAST(s.margin AS DOUBLE) >= 0 AND CAST(s.size AS DOUBLE) = 0 AND CAST(s.tradeSize AS DOUBLE) < 0 AND s.size != s.tradeSize) THEN 'close'
		WHEN (CAST(s.margin AS DOUBLE) >= 0 AND CAST(s.size AS DOUBLE) = 0 AND CAST(s.tradeSize AS DOUBLE) > 0 AND s.size != s.tradeSize) THEN 'close'
		WHEN CAST(s.tradeSize AS DOUBLE) > 0 THEN 'long'
		WHEN CAST(s.tradeSize AS DOUBLE) < 0 THEN 'short'
		ELSE 'NA'
		END AS trade
		,'Synthetix' AS project
		,'2' AS version
		,COALESCE(
			CONCAT(
					UPPER(SUBSTRING(from_utf8(tr.trackingCode), 1, 1)),
					LOWER(SUBSTRING(from_utf8(tr.trackingCode), 2))
			), 
			'Unspecified'		
		) as frontend
		,s.account AS trader
		,cast(ABS(s.tradeSize) as UINT256) AS volume_raw
		,s.evt_tx_hash AS tx_hash
		,s.evt_index
	FROM {{ source('synthetix_futuresmarket_optimism', 'ProxyPerpsV2_evt_PositionModified') }} AS s
	LEFT JOIN synthetix_markets AS sm
		ON s.contract_address = sm.market
	LEFT JOIN asset_price AS p
		ON s.contract_address = p.market_address
		AND s.evt_block_time = p.evt_block_time
	LEFT JOIN {{ source('synthetix_futuresmarket_optimism', 'ProxyPerpsV2_evt_PerpsTracking') }} AS tr
		ON s.evt_tx_hash = tr.evt_tx_hash
		AND s.fee = tr.fee
		AND s.tradeSize = tr.sizeDelta
	WHERE CAST(s.tradeSize AS DOUBLE) != 0
	{% if is_incremental() %}
	AND s.evt_block_time >= DATE_TRUNC('DAY', NOW() - INTERVAL '7' Day)
	{% endif %}
)

SELECT
	'optimism' AS blockchain
	,CAST(date_trunc('DAY', perps.block_time) AS date) AS block_date
	,CAST(date_trunc('MONTH', perps.block_time) AS date) AS block_month
	,perps.block_time
	,perps.virtual_asset
	,cast(perps.underlying_asset as VARCHAR) as underlying_asset
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