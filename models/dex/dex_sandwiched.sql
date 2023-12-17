{{ config(
        
        schema='dex',
        alias = 'sandwiched',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zksync"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

{% set sandwiched_models = [
     (ref('dex_arbitrum_sandwiched'))
     , (ref('dex_avalanche_c_sandwiched'))
     , (ref('dex_bnb_sandwiched'))
     , (ref('dex_ethereum_sandwiched'))
     , (ref('dex_fantom_sandwiched'))
     , (ref('dex_gnosis_sandwiched'))
     , (ref('dex_optimism_sandwiched'))
     , (ref('dex_polygon_sandwiched'))
     , (ref('dex_base_sandwiched'))
     , (ref('dex_celo_sandwiched'))
     , (ref('dex_zksync_sandwiched'))
] %}

SELECT *
FROM (
        {% for sandwiched_model in sandwiched_models %}
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
        FROM {{ sandwiched_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )