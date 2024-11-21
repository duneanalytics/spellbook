{{ config(
        
        schema='gas',
        alias = 'prices',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'minute'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zora", "zksync", "scroll", "linea", "zkevm"]\',
                                "sector",
                                "gas",
                                \'["hildobby"]\') }}'
        )
}}

{% set all_models = [
     (ref('gas_arbitrum_prices'))
     , (ref('gas_avalanche_c_prices'))
     , (ref('gas_bnb_prices'))
     , (ref('gas_ethereum_prices'))
     , (ref('gas_fantom_prices'))
     , (ref('gas_gnosis_prices'))
     , (ref('gas_optimism_prices'))
     , (ref('gas_polygon_prices'))
     , (ref('gas_base_prices'))
     , (ref('gas_celo_prices'))
     , (ref('gas_zora_prices'))
     , (ref('gas_zksync_prices'))
     , (ref('gas_scroll_prices'))
     , (ref('gas_goerli_prices'))
     , (ref('gas_linea_prices'))
     , (ref('gas_zkevm_prices'))
] %}

SELECT *
FROM (
        {% for all_model in all_models %}
        SELECT blockchain
        , minute
        , median_gas
        , tenth_percentile_gas
        , ninetieth_percentile_gas
        , avg_gas
        , min_gas
        , max_gas
        FROM {{ all_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('minute') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )