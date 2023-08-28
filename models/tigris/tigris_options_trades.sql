{{ config(
	tags=['dunesql', 'prod_exclude'],
	alias = alias('options_trades'),
    post_hook='{{ expose_spells(\'["arbitrum", "polygon"]\',
                                "project",
                                "tigris",
                                \'["Henrystats"]\') }}'
	)
}}

{% set options_models = [
 ref('tigris_arbitrum_options_trades')
 ,ref('tigris_polygon_options_trades')
] %}

SELECT *
FROM (
    {% for trade_model in options_models %}
    SELECT
		blockchain
		,day
		,block_month
		,evt_block_time
		,volume_usd
		,position_id
		,open_price
        ,close_price
		,version
        ,profitnLoss
        ,collateral_amount
        ,collateral_asset
        ,pair
        ,options_period
        ,referral
		,trader
		,evt_tx_hash
		,evt_index
        ,trade_direction
        ,trade_type
        ,protocol_version
        ,project_contract_address
        ,positions_contract
        ,fees
    FROM {{ trade_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
