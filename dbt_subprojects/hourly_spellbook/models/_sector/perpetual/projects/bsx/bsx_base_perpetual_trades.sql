{{ config(
    schema = 'bsx_perpetual_trades',
    alias = 'perpetual_trades',
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                    spell_type = "project",
                                    spell_name = "bsx",
                                    contributors = \'["princi"]\') }}'
        )
}}

{% set bsx_base_perpetual_trade_models = [
    ref('bsx_v1_base_perpetual_trades')
] %}

SELECT *
FROM
(
    {% for bsx_perpetual_trades in bsx_base_perpetual_trade_models %}
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
    FROM {{ bsx_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)   