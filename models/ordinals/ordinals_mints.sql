{{ config(
        
        schema='ordinals',
        alias = 'mints',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zora", "zksync"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{% set sandwiches_models = [
     (ref('ordinals_arbitrum_mints'))
     , (ref('ordinals_avalanche_c_mints'))
     , (ref('ordinals_bnb_mints'))
     , (ref('ordinals_ethereum_mints'))
     , (ref('ordinals_fantom_mints'))
     , (ref('ordinals_gnosis_mints'))
     , (ref('ordinals_optimism_mints'))
     , (ref('ordinals_polygon_mints'))
     , (ref('ordinals_base_mints'))
     , (ref('ordinals_celo_mints'))
     , (ref('ordinals_zora_mints'))
     , (ref('ordinals_zksync_mints'))
] %}

SELECT *
FROM (
        {% for sandwiches_model in sandwiches_models %}
        SELECT blockchain
        , block_time
        , block_month
        , block_number
        , tx_hash
        , tx_from
        , tx_to
        , tx_index
        , action
        , ordinal_standard
        , operation
        , ordinal_symbol
        , amount
        , full_inscription
        FROM {{ sandwiches_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )