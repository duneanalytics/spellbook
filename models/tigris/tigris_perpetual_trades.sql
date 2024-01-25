{{ config(
	
	alias = 'perpetual_trades',
    post_hook='{{ expose_spells(\'["arbitrum", "polygon"]\',
                                "project",
                                "tigris",
                                \'["Henrystats"]\') }}'
	)
}}

{% set perpetual_models = [
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
		,positions_contract
		,position_id
    FROM {{ trade_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

-- reload