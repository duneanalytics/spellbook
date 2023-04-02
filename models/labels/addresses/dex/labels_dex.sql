{{ config(
    alias = 'dex',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                "sector",
                                "labels",
                                \'["ilemi"]\') }}')
}}

{% set dex_models = [
 ref('labels_sandwich_attackers')
,ref('labels_dex_aggregator_traders')
,ref('labels_arbitrage_traders')
,ref('labels_dex_traders')
,ref('labels_smart_dex_traders')
,ref('labels_trader_platforms')

,ref('labels_average_trade_values')
,ref('labels_trader_age')
,ref('labels_trader_dex_diversity')
,ref('labels_trader_frequencies')
,ref('labels_trader_portfolios')
] %}

SELECT *
FROM (
    {% for dex_model in dex_models %}
    SELECT
        *
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
