{{ config(
    schema = 'dip_exchange_base',
    alias = 'perpetual_trades',
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                    spell_type = "project",
                                    spell_name = "dip_exchange",
                                    contributors = \'["princi"]\') }}'
        )
}}

{% set dip_exchange_base_perpetual_trade_models = [
    ref('dip_exchange_v1_base_perpetual_trades')
] %}

SELECT *
FROM
(
    {% for dip_exchange_perpetual_trades in dip_exchange_base_perpetual_trade_models %}
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
    FROM {{ dip_exchange_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)   