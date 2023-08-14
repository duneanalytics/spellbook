{{ config(
	tags=['legacy'],

    schema = 'balancer',
    alias = alias('bpt_prices_legacy', legacy_model=True),
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon"]\',
                            "project",
                            "balancer",
                            \'["thetroyharris"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_bpt_prices_legacy')
    , ref('balancer_v2_avalanche_c_bpt_prices_legacy')
    , ref('balancer_v2_base_bpt_prices_legacy')
    , ref('balancer_v2_ethereum_bpt_prices_legacy')
    , ref('balancer_v2_gnosis_bpt_prices_legacy')
    , ref('balancer_v2_optimism_bpt_prices_legacy')
    , ref('balancer_v2_polygon_bpt_prices_legacy')
] %}

SELECT *
FROM (
    {% for bpt_prices_legacy in balancer_models %}
    SELECT
        blockchain,
        hour,
        contract_address,
        median_price
    FROM {{ bpt_prices_legacy }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
