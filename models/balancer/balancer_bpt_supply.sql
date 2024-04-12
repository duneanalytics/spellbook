{{ config(
    schema = 'balancer',
    alias = 'bpt_supply',
    post_hook = '{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon"]\',
                                "project",
                                "balancer",
                                \'["thetroyharris", "viniabussafi"]\') }}'
    )
}}

{% set bpt_supply_models = [
    ref('balancer_v2_ethereum_bpt_supply'),
    ref('balancer_v2_arbitrum_bpt_supply'),
    ref('balancer_v2_polygon_bpt_supply'),
    ref('balancer_v2_gnosis_bpt_supply'),
    ref('balancer_v2_optimism_bpt_supply'),
    ref('balancer_v2_avalanche_c_bpt_supply'),
    ref('balancer_v2_base_bpt_supply') 
] %}

SELECT *
FROM (
    {% for bpt_supply_model in bpt_supply_models %}
    SELECT
        day,
        token_address,
        pool_type,
        version,
        blockchain,
        supply
    FROM {{ bpt_supply_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
);
