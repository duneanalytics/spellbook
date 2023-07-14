{{ config(
        alias = alias('addresses', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "optimism", "arbitrum", "polygon", "bitcoin", "fantom"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')
}}


{% set cex_models = [
ref('cex_arbitrum_addresses_legacy')
, ref('cex_avalanche_c_addresses_legacy')
, ref('cex_bitcoin_addresses_legacy')
, ref('cex_bnb_addresses_legacy')
, ref('cex_ethereum_addresses_legacy')
, ref('cex_fantom_addresses_legacy')
, ref('cex_optimism_addresses_legacy')
, ref('cex_polygon_addresses_legacy')
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