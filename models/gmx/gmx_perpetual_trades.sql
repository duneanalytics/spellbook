{{ config(
		
        alias = 'perpetual_trades',
        post_hook='{{ expose_spells(\'["avalanche_c","arbitrum"]\',
                                "project",
                                "gmx",
                                \'["Henrystats"]\') }}'
        )
}}

{% set gmx_perp_models = [
ref('gmx_avalanche_c_perpetual_trades')
, ref('gmx_arbitrum_perpetual_trades')
] %}


SELECT *
FROM (
    {% for perpetual_model in gmx_perp_models %}
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
    FROM {{ perpetual_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)