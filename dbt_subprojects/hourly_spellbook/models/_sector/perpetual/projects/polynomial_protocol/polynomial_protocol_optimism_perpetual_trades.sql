{{ config(
    schema = 'polynomial_protocol_optimism',
    alias = 'perpetual_trades',
    post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                    spell_type = "project",
                                    spell_name = "polynomial_protocol",
                                    contributors = \'["princi"]\') }}'
        )
}}

{% set polynomial_protocol_optimism_perpetual_trade_models = [
    ref('polynomial_protocol_v1_optimism_perpetual_trades')
] %}

SELECT *
FROM
(
    {% for polynomial_protocol_perpetual_trades in polynomial_protocol_optimism_perpetual_trade_models %}
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
    FROM {{ polynomial_protocol_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)   