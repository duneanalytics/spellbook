{{ config(
        
        schema='inscription',
        alias = 'deploys',
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

{% set deploys_models = [
     (ref('inscription_arbitrum_deploys'))
     , (ref('inscription_avalanche_c_deploys'))
     , (ref('inscription_bnb_deploys'))
     , (ref('inscription_ethereum_deploys'))
     , (ref('inscription_fantom_deploys'))
     , (ref('inscription_gnosis_deploys'))
     , (ref('inscription_optimism_deploys'))
     , (ref('inscription_polygon_deploys'))
     , (ref('inscription_base_deploys'))
     , (ref('inscription_celo_deploys'))
     , (ref('inscription_zora_deploys'))
     , (ref('inscription_scroll_deploys'))
     , (ref('inscription_zksync_deploys'))
     , (ref('inscription_goerli_deploys'))
] %}

SELECT *
FROM (
        {% for deploys_model in deploys_models %}
        SELECT blockchain
        , block_time
        , block_month
        , block_number
        , tx_hash
        , tx_from
        , tx_to
        , tx_index
        , inscription_standard
        , operation
        , inscription_symbol
        , max_supply
        , mint_limit
        , full_inscription
        
        FROM {{ deploys_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )