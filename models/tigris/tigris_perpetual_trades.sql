{{ config(
	tags=['dunesql'],
	alias = alias('perpetual_trades'),
    post_hook='{{ expose_spells(\'["arbitrum", "polygon"]\',
                                "project",
                                "tigris",
                                \'["Henrystats"]\') }}'
	)
}}

{% set perpetual_modes = [
 ref('tigris_arbitrum_perpetual_trades')
 ,ref('tigris_polygon_perpetual_trades')
] %}


SELECT *
FROM (
    {% for trade_model in perpetual_models %}
    SELECT
		blockchain
		,block_date
		,block_month
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
        ,protocol_version
    FROM {{ trade_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)