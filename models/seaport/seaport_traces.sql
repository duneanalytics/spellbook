{{ config(
        tags = ['dunesql'],
        alias = alias('traces'),
        unique_key = ['blockchain', 'block_number', 'tx_hash', 'evt_index', 'order_hash', 'trace_side', 'trace_index'],
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}'
)
}}

{% set seaport_models = [
ref('seaport_arbitrum_traces')
, ref('seaport_avalanche_c_traces')
, ref('seaport_base_traces')
, ref('seaport_bnb_traces')
, ref('seaport_ethereum_traces')
, ref('seaport_optimism_traces')
, ref('seaport_polygon_traces')
] %}

SELECT *
FROM (
    {% for seaport_model in seaport_models %}
    SELECT blockchain
    , block_date
    , block_time
    , block_number
    , order_hash
    , amount
    , identifier
    , offerer
    , recipient
    , token_address
    , token_standard
    , trace_index
    , trace_side
    , tx_hash
    , zone
    , evt_index
    , seaport_contract_address
    , seaport_version
    FROM {{ seaport_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )