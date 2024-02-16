{{ config(
        
        schema='inscription',
        alias = 'mints',
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

{% set mints_models = [
     (ref('inscription_arbitrum_mints'))
     , (ref('inscription_avalanche_c_mints'))
     , (ref('inscription_bnb_mints'))
     , (ref('inscription_ethereum_mints'))
     , (ref('inscription_fantom_mints'))
     , (ref('inscription_gnosis_mints'))
     , (ref('inscription_optimism_mints'))
     , (ref('inscription_polygon_mints'))
     , (ref('inscription_base_mints'))
     , (ref('inscription_celo_mints'))
     , (ref('inscription_zora_mints'))
     , (ref('inscription_scroll_mints'))
     , (ref('inscription_zksync_mints'))
     , (ref('inscription_goerli_mints'))
] %}

SELECT *
FROM (
        {% for mints_model in mints_models %}
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
        , amount
        , full_inscription
        FROM {{ mints_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )