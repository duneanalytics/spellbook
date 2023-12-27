{{ config(
	
	alias = 'all_perpetual_trades',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "mummy_finance",
                                \'["kaiblade"]\') }}'
	)
}}

{% set mummy_finance_optimism_perpetual_trade_models = [
 ref('mummy_finance_optimism_v1_perpetual_trades')
] %}


SELECT *
FROM (
    {% for perpetual_trade in mummy_finance_optimism_perpetual_trade_models %}
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
    FROM {{ perpetual_trade }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)