{{ config(
	tags=['legacy'],
	
	schema = 'synthetix_v1_optimism',
	alias = alias('perpetual_trades', legacy_model=True),
	partition_by = ['block_date'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "synthetix",
                                \'["msilb7", "drethereum", "rplust"]\') }}'
	)
}}

{% set project_start_date = '2021-11-22' %}

WITH asset_price AS (
	SELECT
		s.contract_address AS market_address
		,DECODE(
			UNHEX(
				SUBSTRING(sm.asset, 3)
				), 'UTF-8'
			) AS asset
		,s.evt_block_time
		,AVG(s.lastPrice/1e18) AS price
	FROM {{ source('synthetix_optimism', 'FuturesMarket_evt_PositionModified') }} AS s
	LEFT JOIN {{ source('synthetix_optimism', 'FuturesMarketManager_evt_MarketAdded') }} AS sm
		ON s.contract_address = sm.market
	{% if is_incremental() %}
	WHERE s.evt_block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
	{% endif %}
	GROUP BY market_address, asset, s.evt_block_time
),

synthetix_markets AS (
	SELECT DISTINCT
		--finds the position of the first occurence '00' in the hex string which indicates null characters/padded zeroes
		--characters before this position should be taken to get the asset's hex name; use 'unhex' to get the readable text

		--if the position is on an even number, that means the first '0' is part of the hexed version of the asset's last letter
		--in this case, this zero should be included in the hex characters to be unhexed to get the complete asset's name

		--substring starts on 3 to skip the '0x' at the beginning of the string
		CASE
			WHEN MOD(POSITION('00' IN SUBSTRING(asset, 3)), 2) = 0 THEN UNHEX(SUBSTRING(asset, 3, POSITION('00' IN SUBSTRING(asset, 3))))
			ELSE UNHEX(SUBSTRING(asset, 3, POSITION('00' IN SUBSTRING(asset, 3))-1))
		END AS asset
		
		,market
		
		,CASE
			WHEN MOD(POSITION('00' IN SUBSTRING(marketKey, 3)), 2) = 0 THEN UNHEX(SUBSTRING(marketKey, 3, POSITION('00' IN SUBSTRING(marketKey, 3))))
			ELSE UNHEX(SUBSTRING(asset, 3, POSITION('00' IN SUBSTRING(marketKey, 3))-1))
		END AS marketKey
	FROM {{ source('synthetix_optimism', 'FuturesMarketManager_evt_MarketAdded') }}
),

perps AS (
	SELECT
		s.evt_block_time AS block_time
		,s.evt_block_number AS block_number
		,DECODE(sm.asset, 'UTF-8') AS virtual_asset
		
		,CASE
			WHEN LEFT(sm.asset, 1) = 's' THEN SUBSTRING(sm.asset, 2) --removes 's' indicator from synthetic assets
			ELSE sm.asset
		END AS underlying_asset

		,DECODE(sm.marketKey, 'UTF-8') AS market
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
		,'1' AS version
		,INITCAP(IFNULL(DECODE(UNHEX(SUBSTRING(tr.trackingCode, 3)), 'UTF-8'), 'Unspecified')) AS frontend
		,s.account AS trader
		,cast(s.tradeSize as double) AS volume_raw
		,s.evt_tx_hash AS tx_hash
		,s.evt_index
	FROM {{ source('synthetix_optimism', 'FuturesMarket_evt_PositionModified') }} AS s
	LEFT JOIN synthetix_markets AS sm
		ON s.contract_address = sm.market
	LEFT JOIN asset_price AS p
		ON s.contract_address = p.market_address
		AND s.evt_block_time = p.evt_block_time
	LEFT JOIN {{ source('synthetix_optimism', 'FuturesMarket_evt_FuturesTracking') }} AS tr
		ON s.evt_tx_hash = tr.evt_tx_hash
		AND s.fee = tr.fee
		AND s.tradeSize = tr.sizeDelta
	WHERE CAST(s.tradeSize AS DOUBLE) != 0
	{% if is_incremental() %}
	AND s.evt_block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
	{% endif %}
)

SELECT
	'optimism' AS blockchain
	,TRY_CAST(date_trunc('DAY', perps.block_time) AS date) AS block_date
	,perps.block_time
	,perps.virtual_asset
	,cast(perps.underlying_asset as string)
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
	AND tx.block_time >= DATE_TRUNC("DAY", NOW() - INTERVAL '1 WEEK')
	{% endif %}