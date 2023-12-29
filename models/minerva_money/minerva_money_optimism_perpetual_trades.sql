{{ config(
	schema = 'minerva_money_optimism',
	alias = 'all_perpetual_trades',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "minerva_money",
                                \'["kaiblade"]\') }}'
	)
}}

{% set opx_finance_optimism_perpetual_trade_models = [
 ref('minerva_money_optimism_v1_perpetual_trades')
] %}


SELECT *
FROM (
    {% for perpetual_trades in opx_finance_optimism_perpetual_trade_models %}
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