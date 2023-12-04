{{ config(
        
        schema='inscriptions',
        alias = 'mints',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zora", "zksync"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{% set sandwiches_models = [
     (ref('inscriptions_arbitrum_mints'))
     , (ref('inscriptions_avalanche_c_mints'))
     , (ref('inscriptions_bnb_mints'))
     , (ref('inscriptions_ethereum_mints'))
     , (ref('inscriptions_fantom_mints'))
     , (ref('inscriptions_gnosis_mints'))
     , (ref('inscriptions_optimism_mints'))
     , (ref('inscriptions_polygon_mints'))
     , (ref('inscriptions_base_mints'))
     , (ref('inscriptions_celo_mints'))
     , (ref('inscriptions_zora_mints'))
     , (ref('inscriptions_zksync_mints'))
] %}

SELECT *
FROM (
        {% for sandwiches_model in sandwiches_models %}
        SELECT blockchain
        , project
        , version
        , block_time
        , block_month
        , block_number
        , token_sold_address
        , token_bought_address
        , token_sold_symbol
        , token_bought_symbol
        , maker
        , taker
        , tx_hash
        , tx_from
        , tx_to
        , project_contract_address
        , token_pair
        , tx_index
        , token_sold_amount_raw
        , token_bought_amount_raw
        , token_sold_amount
        , token_bought_amount
        , amount_usd
        , evt_index
        FROM {{ sandwiches_model }}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )