{{ config(
    schema = 'lyra_perpetual_trades',
    alias = 'perpetual_trades',
    post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                    spell_type = "project",
                                    spell_name = "lyra",
                                    contributors = \'["princi"]\') }}'
        )
}}

{% set lyra_optimism_perpetual_trade_models = [
    ref('lyra_v1_optimism_perpetual_trades')
] %}

SELECT *
FROM
(
    {% for lyra_perpetual_trades in lyra_optimism_perpetual_trade_models %}
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
    FROM {{ lyra_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)   