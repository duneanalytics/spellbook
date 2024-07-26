{{ config(

    alias = 'dex',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                "sector",
                                "labels",
                                \'["ilemi", "kaiblade"]\') }}')
}}

{% set dex_models = [
 ref('labels_sandwich_attackers')
,ref('labels_dex_aggregator_traders')
,ref('labels_arbitrage_traders')
,ref('labels_dex_traders')
,ref('labels_smart_dex_traders')
,ref('labels_trader_platforms')
,ref('labels_dex_pools')
,ref('labels_trader_kyt')
,ref('labels_average_trade_values')
,ref('labels_trader_age')
,ref('labels_trader_dex_diversity')
,ref('labels_trader_frequencies')
,ref('labels_op_dex_traders')
] %}

SELECT *
FROM (
    {% for dex_model in dex_models %}
    SELECT
        blockchain
        , address
        , name
        , category
        , contributor
        , source
        , created_at
        , updated_at
        , model_name
        , label_type
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
