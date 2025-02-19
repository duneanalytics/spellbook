{{ config(
    schema = 'yfx_base',
    alias = 'perpetual_trades',
    post_hook='{{ expose_spells(\'["base"]\',
                                    "project",
                                    "yfx",
                                    \'["principatel"]\') }}'
        )
}}

{% set yfx_base_perpetual_trade_models = [
    ref('yfx_v4_base_perpetual_trades')
] %}

SELECT *
FROM
(
    {% for yfx_perpetual_trades in yfx_base_perpetual_trade_models %}
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
    FROM {{ yfx_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)