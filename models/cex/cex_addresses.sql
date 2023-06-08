{{ config(
        alias ='addresses',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "optimism", "arbitrum", "polygon", "bitcoin", "fantom"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')
}}


{% set cex_models = [
ref('cex_arbitrum_addresses')
, ref('cex_avalanche_c_addresses')
, ref('cex_bitcoin_addresses')
, ref('cex_bnb_addresses')
, ref('cex_ethereum_addresses')
, ref('cex_fantom_addresses')
, ref('cex_optimism_addresses')
, ref('cex_polygon_addresses')
] %}

SELECT *
FROM (
    {% for cex_model in cex_models %}
    SELECT
        blockchain, 
        address,
        cex_name,
        distinct_name,
        added_by,
        added_date
    FROM {{ cex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)