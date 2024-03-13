{{ config(
    schema = 'synthetix_v3_base',
	alias = 'perpetual_trades',
	partition_by = ['block_month'],
	materialized = 'incremental',
	file_format = 'delta',
	incremental_strategy = 'merge',
	unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
	)
}}

{% set project_start_date = '2023-12-19' %}

WITH 

synthetix_accounts as (
    SELECT 
        accountId,
        owner
    FROM 
    {{ source('synthetix_v3_base', 'PerpsMarket_evt_AccountCreated') }}
),

synthetix_markets as (
    SELECT 
        marketName,
        marketSymbol,
        perpsMarketId 
    FROM 
    {{ source('synthetix_v3_base', 'PerpsMarket_evt_MarketCreated') }}
),

perps as (
    SELECT 
        mu.evt_block_time as block_time,
        mu.evt_block_number as block_number,
        sm.marketSymbol as virtual_asset,
        'snxUSD' as underlying_asset, -- this is same for all markets
        sm.marketName as market,
        mu.contract_address as market_address,
        ABS(mu.sizeDelta)/1e18 * mu.price/1e18 as volume_usd, 
        os.totalFees/1e18 as fee_usd,
        mu.size/1e18 * mu.price/1e18 as margin_usd,
        (ABS(mu.sizeDelta)/1e18 * mu.price/1e18) / (mu.size/1e18 * mu.price/1e18) as leverage_ratio,
        CASE 
            WHEN (CAST(mu.size AS DOUBLE) >= 0 AND CAST(os.newSize AS DOUBLE) = 0 AND CAST(mu.sizeDelta AS DOUBLE) < 0 AND os.newSize != mu.sizeDelta) THEN 'close'
		    WHEN (CAST(mu.size AS DOUBLE) >= 0 AND CAST(os.newSize AS DOUBLE) = 0 AND CAST(mu.sizeDelta AS DOUBLE) > 0 AND os.newSize != mu.sizeDelta) THEN 'close'
        	WHEN CAST(mu.sizeDelta AS DOUBLE) > 0 THEN 'long'
		    WHEN CAST(mu.sizeDelta AS DOUBLE) < 0 THEN 'short'
		    ELSE 'NA'
		END AS trade,
		'Synthetix' AS project,
		'3' AS version,
		COALESCE(
			CONCAT(
					UPPER(SUBSTRING(from_utf8(os.trackingCode), 1, 1)),
					LOWER(SUBSTRING(from_utf8(os.trackingCode), 2))
			), 
			'Unspecified'		
		) as frontend,
        sa.owner as trader,
        CAST(ABS(mu.sizeDelta) as UINT256) as volume_raw,
        mu.evt_tx_hash as tx_hash,
        mu.evt_index
    FROM 
    {{ source('synthetix_v3_base', 'PerpsMarket_evt_MarketUpdated') }} mu 
    INNER JOIN 
    {{ source('synthetix_v3_base', 'PerpsMarket_evt_OrderSettled') }} os 
        ON mu.evt_tx_hash = os.evt_tx_hash
        AND mu.sizeDelta = os.sizeDelta
        {% if is_incremental() %}
        AND {{incremental_predicate('os.evt_block_time')}}
        {% endif %}
    INNER JOIN 
    synthetix_accounts sa 
        ON os.accountId = sa.accountId
    INNER JOIN 
    synthetix_markets sm 
        ON mu.marketId = sm.perpsMarketId
    WHERE CAST(mu.sizeDelta AS DOUBLE) != 0
    {% if is_incremental() %}
    AND {{incremental_predicate('mu.evt_block_time')}}
    {% endif %}
)

SELECT
	'base' AS blockchain
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
INNER JOIN {{ source('base', 'transactions') }} AS tx
	ON perps.tx_hash = tx.hash
	AND perps.block_number = tx.block_number
	{% if not is_incremental() %}
	AND tx.block_time >= DATE '{{project_start_date}}'
	{% endif %}
	{% if is_incremental() %}
	AND {{incremental_predicate('tx.block_time')}}
	{% endif %}

