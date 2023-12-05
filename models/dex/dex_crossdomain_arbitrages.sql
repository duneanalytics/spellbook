{{ config(
        schema='dex',
        alias = 'crossdomain_arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{% set crossdomain_arbitrages_models = [
     (ref('dex_arbitrum_crossdomain_arbitrages'))
     , (ref('dex_avalanche_c_crossdomain_arbitrages'))
     , (ref('dex_bnb_crossdomain_arbitrages'))
     , (ref('dex_ethereum_crossdomain_arbitrages'))
     , (ref('dex_fantom_crossdomain_arbitrages'))
     , (ref('dex_gnosis_crossdomain_arbitrages'))
     , (ref('dex_optimism_crossdomain_arbitrages'))
     , (ref('dex_polygon_crossdomain_arbitrages'))
     , (ref('dex_base_crossdomain_arbitrages'))
] %}

SELECT *
FROM (
        {% for crossdomain_arbitrages_model in crossdomain_arbitrages_models %}
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
        , index
        , token_sold_amount_raw
        , token_bought_amount_raw
        , token_sold_amount
        , token_bought_amount
        , amount_usd
        , evt_index
        FROM {{ crossdomain_arbitrages_model }}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )