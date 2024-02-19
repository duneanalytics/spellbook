{{ config(
        
        schema='inscription',
        alias = 'all',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zora", "zksync", "scroll"]\',
                                "sector",
                                "inscription",
                                \'["hildobby"]\') }}'
        )
}}

{% set all_models = [
     (ref('inscription_arbitrum_all'))
     , (ref('inscription_avalanche_c_all'))
     , (ref('inscription_bnb_all'))
     , (ref('inscription_ethereum_all'))
     , (ref('inscription_fantom_all'))
     , (ref('inscription_gnosis_all'))
     , (ref('inscription_optimism_all'))
     , (ref('inscription_polygon_all'))
     , (ref('inscription_base_all'))
     , (ref('inscription_celo_all'))
     , (ref('inscription_zora_all'))
     , (ref('inscription_zksync_all'))
     , (ref('inscription_scroll_all'))
     , (ref('inscription_goerli_all'))
] %}

SELECT *
FROM (
        {% for all_model in all_models %}
        SELECT blockchain
        , block_time
        , block_month
        , block_number
        , tx_hash
        , tx_index
        , tx_from
        , tx_to
        , full_inscription
        FROM {{ all_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )