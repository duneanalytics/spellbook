{{ config(
	schema = 'fxdx_optimism',
	alias = 'perpetual_trades'
	)
}}

{% set fxdx_optimism_perpetual_trade_models = [
 ref('fxdx_v2_optimism_perpetual_trades')
] %}


SELECT *
FROM (
    {% for perpetual_trades in fxdx_optimism_perpetual_trade_models %}
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
    FROM {{ perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)