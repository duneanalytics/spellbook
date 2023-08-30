{{ config(
        tags = ['dunesql'],
        alias = alias('traces'),
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
, ref('seaport_arbitrum_traces')
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
    , trace_side
    , order_hash
    , tx_hash
    , token_standard
    , trace_index
    , seaport_contract_address
    , seaport_version
    , token_address
    , amount
    , bundle_size
    , identifier
    , recipient
    , offerer
    , zone
    FROM {{ seaport_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )