{{ config(
    schema = 'balancer',
    alias = 'bpt_prices',
    post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "polygon", "gnosis", "optimism"]\',
                            "project",
                            "balancer",
                            \'["thetroyharris"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_ethereum_bpt_prices'),
    ref('balancer_v2_arbitrum_bpt_prices'),
    ref('balancer_v2_polygon_bpt_prices'),
    ref('balancer_v2_gnosis_bpt_prices'),
    ref('balancer_v2_optimism_bpt_prices'),
] %}

SELECT *
FROM (
    {% for bpt_prices in balancer_models %}
    SELECT
        blockchain,
        hour,
        contract_address,
        median_price
    FROM {{ bpt_prices }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
