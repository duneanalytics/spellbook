{{ config(
        tags=['dunesql'],
        schema='dex',
        alias = alias('crossdomain_mev'),
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

{% set crossdomain_mev_models = [
     (ref('dex_arbitrum_crossdomain_mev'))
     , (ref('dex_avalanche_c_crossdomain_mev'))
     , (ref('dex_bnb_crossdomain_mev'))
     , (ref('dex_ethereum_crossdomain_mev'))
     , (ref('dex_fantom_crossdomain_mev'))
     , (ref('dex_gnosis_crossdomain_mev'))
     , (ref('dex_optimism_crossdomain_mev'))
     , (ref('dex_polygon_crossdomain_mev'))
     , (ref('dex_base_crossdomain_mev'))
] %}

SELECT *
FROM (
        {% for crossdomain_mev_model in crossdomain_mev_models %}
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
        FROM {{ crossdomain_mev_model }}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )