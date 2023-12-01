{{ config(
        schema='dex',
        alias = 'arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zksync"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{% set arbitrages_models = [
     (ref('dex_arbitrum_arbitrages'))
     , (ref('dex_avalanche_c_arbitrages'))
     , (ref('dex_bnb_arbitrages'))
     , (ref('dex_ethereum_arbitrages'))
     , (ref('dex_fantom_arbitrages'))
     , (ref('dex_gnosis_arbitrages'))
     , (ref('dex_optimism_arbitrages'))
     , (ref('dex_polygon_arbitrages'))
     , (ref('dex_base_arbitrages'))
     , (ref('dex_celo_arbitrages'))
     , (ref('dex_zksync_arbitrages'))
] %}

SELECT *
FROM (
        {% for arbitrages_model in arbitrages_models %}
        SELECT block_time
        , txs.block_number
        , tx_hash
        , evt_index
        , blockchain
        , project
        , version
        , block_month
        , token_sold_symbol
        , token_bought_symbol
        , token_sold_address
        , token_bought_address
        , token_pair
        , token_sold_amount
        , token_bought_amount
        , token_sold_amount_raw
        , token_bought_amount_raw
        , amount_usd
        , taker
        , maker
        , project_contract_address
        , tx_from
        , tx_to
        , tx_index
        FROM {{ arbitrages_model }}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )