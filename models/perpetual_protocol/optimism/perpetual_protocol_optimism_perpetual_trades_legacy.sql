{{ config(
	tags=['legacy'],
	
	alias = alias('perpetual_trades', legacy_model=True),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "perpetual_protocol",
                                \'["msilb7", "drethereum", "rplust"]\') }}'
	)
}}

{% set perpetual_protocol_optimism_perpetual_trade_models = [
 ref('perpetual_protocol_v2_optimism_perpetual_trades_legacy')
] %}

SELECT *
FROM (
    {% for perpetual_protocol_perpetual_trades in perpetual_protocol_optimism_perpetual_trade_models %}
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
		,frontend
		,trader
		,volume_raw
		,tx_hash
		,tx_from
		,tx_to
		,evt_index
    FROM {{ perpetual_protocol_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)