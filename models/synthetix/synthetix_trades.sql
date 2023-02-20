{{ config(
	alias = 'trades',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "synthetix",
                                \'["msilb7", "drethereum", "rplust"]\') }}'
	)
}}

SELECT *
FROM
(
	SELECT
		blockchain
		,block_date
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
	FROM {{ ref('synthetix_optimism_trades') }}
)