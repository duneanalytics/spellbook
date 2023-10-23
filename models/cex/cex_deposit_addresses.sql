{{ config(
        tags = ['dunesql'],
        schema = 'cex',
        alias = alias('deposit_addresses'),
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "optimism", "arbitrum", "polygon", "bitcoin", "fantom"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')
}}


{% set cex_models = [
ref('cex_arbitrum_deposit_addresses')
, ref('cex_avalanche_c_deposit_addresses')
, ref('cex_bnb_deposit_addresses')
, ref('cex_ethereum_deposit_addresses')
, ref('cex_fantom_deposit_addresses')
, ref('cex_optimism_deposit_addresses')
, ref('cex_polygon_deposit_addresses')
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