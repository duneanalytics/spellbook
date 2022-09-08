{{ config(
	alias = 'trades'
	)
}}

SELECT
	blockchain
	,block_time
	,virtual_asset
	,underlying_asset
	,market
	,market_address
	,volume_usd
	,fee_usd
	,margin_usd
	,trade
	,project
	,version
	,trader
	,volume_raw
	,tx_hash
	,tx_from
	,tx_to
	,evt_index
FROM {{ ref('perpetual_optimism_trades') }}