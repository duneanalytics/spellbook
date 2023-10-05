{{ config(
	tags=['legacy'],
	
    alias = alias('dex', legacy_model=True),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                "sector",
                                "labels",
                                \'["ilemi"]\') }}')
}}

{% set dex_models = [
 ref('labels_sandwich_attackers_legacy')
,ref('labels_dex_aggregator_traders_legacy')
,ref('labels_arbitrage_traders_legacy')
,ref('labels_dex_traders_legacy')
,ref('labels_smart_dex_traders_legacy')
,ref('labels_trader_platforms_legacy')
,ref('labels_dex_pools_legacy')
,ref('labels_trader_kyt_legacy')
,ref('labels_average_trade_values_legacy')
,ref('labels_trader_age_legacy')
,ref('labels_trader_dex_diversity_legacy')
,ref('labels_trader_frequencies_legacy')
,ref('labels_trader_portfolios_legacy')
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
