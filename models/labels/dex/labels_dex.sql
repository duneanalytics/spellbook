{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                "sector",
                                "labels",
                                \'["ilemi"]\') }}')
}}

{% set dex_labels = [
--Persona
 ref('labels_sandwich_attackers')
,ref('labels_dex_aggregator_traders')
,ref('labels_arbitrage_traders')
--Usage
,ref('labels_average_trade_values')
,ref('labels_trader_age')
] %}

SELECT *
FROM (
    {% for dex_model in dex_trade_models %}
    SELECT
        *
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
