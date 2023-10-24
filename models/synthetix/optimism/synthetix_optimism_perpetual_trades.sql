{{ config(
	
    alias = 'perpetual_trades',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "synthetix",
                                    \'["msilb7", "drethereum", "rplust"]\') }}'
    )
}}

{% set synthetix_optimism_perpetual_trade_models = [
    ref('synthetix_v1_optimism_perpetual_trades')
    , ref('synthetix_v2_optimism_perpetual_trades')
] %}


SELECT *
FROM
(
	{% for synthetix_perpetual_trades in synthetix_optimism_perpetual_trade_models %}
    SELECT
        blockchain
		,block_date
		, block_month
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
        ,frontend
		,trader
		,volume_raw
		,tx_hash
		,tx_from
		,tx_to
		,evt_index
    FROM {{ synthetix_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
