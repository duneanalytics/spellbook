{{ config(
	schema = 'synthetix_base',
    alias = 'perpetual_trades'
    )
}}

{% set synthetix_base_perpetual_trade_models = [
    ref('synthetix_v3_base_perpetual_trades')
] %}


SELECT *
FROM
(
	{% for synthetix_perpetual_trades in synthetix_base_perpetual_trade_models %}
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