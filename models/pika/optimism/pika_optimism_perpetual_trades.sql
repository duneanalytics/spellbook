{{ config(
    tags=['dunesql'],
    alias = alias('perpetual_trades'),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "pika",
                                    \'["msilb7", "drethereum", "rplust"]\') }}'
        )
}}

{% set pika_optimism_perpetual_trade_models = [
    ref('pika_v1_optimism_perpetual_trades')
    , ref('pika_v2_optimism_perpetual_trades')
    , ref('pika_v3_optimism_perpetual_trades')
] %}

SELECT *
FROM
(
    {% for pika_perpetual_trades in pika_optimism_perpetual_trade_models %}
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
    FROM {{ pika_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)