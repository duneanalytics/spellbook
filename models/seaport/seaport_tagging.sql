{{ config(
        tags = [ 'static'],
        alias = 'tagging',
        unique_key = ['blockchain', 'tagging_method', 'identifier'],
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                                    "project",
                                    "seaport",
                                    \'["hildobby"]\') }}'
)
}}

{% set seaport_models = [
ref('seaport_arbitrum_tagging')
, ref('seaport_avalanche_c_tagging')
, ref('seaport_base_tagging')
, ref('seaport_bnb_tagging')
, ref('seaport_ethereum_tagging')
, ref('seaport_optimism_tagging')
, ref('seaport_polygon_tagging')
] %}

SELECT *
FROM (
    {% for seaport_model in seaport_models %}
    SELECT blockchain
    , tagging_method
    , identifier
    , protocol
    , protocol_type
    FROM {{ seaport_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )