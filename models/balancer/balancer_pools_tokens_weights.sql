{{ config(
    schema = 'balancer',
    alias = 'pools_tokens_weights',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon"]\',
                                "project",
                                "balancer",
                                \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_pools_tokens_weights'),
    ref('balancer_v2_avalanche_c_pools_tokens_weights'),
    ref('balancer_v2_base_pools_tokens_weights'),
    ref('balancer_v2_ethereum_pools_tokens_weights'),
    ref('balancer_v2_gnosis_pools_tokens_weights'),
    ref('balancer_v2_optimism_pools_tokens_weights'),
    ref('balancer_v2_polygon_pools_tokens_weights')
] %}


SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
        blockchain,
        version,
        pool_id,
        token_address,
        normalized_weight
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
