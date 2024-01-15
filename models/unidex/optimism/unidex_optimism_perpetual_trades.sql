{{ config(
    alias = 'perpetual_trades',
    schema= 'unidex_optimism'
    )
}}

{% set unidex_optimism_perpetual_trade_models = [
    ref('unidex_v1_optimism_perpetual_trades')
    , ref('unidex_v2_optimism_perpetual_trades')
    , ref('unidex_v3_optimism_perpetual_trades')
] %}

SELECT *
FROM
(
    {% for unidex_perpetual_trades in unidex_optimism_perpetual_trade_models %}
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
    FROM {{ unidex_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)