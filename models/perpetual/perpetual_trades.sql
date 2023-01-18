{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "perpetual",
                                \'["msilb7", "drethereum", "rplust"]\') }}'
        )
}}

{% set perpetual_trade_models = [
 ref('perpetual_protocol_trades')
,ref('pika_trades')
,ref('synthetix_trades')
,ref('gmx_perpetual_trades')
] %}

SELECT *
FROM (
    {% for perpetual_model in perpetual_trade_models %}
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