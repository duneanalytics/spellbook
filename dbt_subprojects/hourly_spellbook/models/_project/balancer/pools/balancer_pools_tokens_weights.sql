{{ config(
    schema = 'balancer',
    alias = 'pools_tokens_weights',
    post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                                spell_type = "project",
                                spell_name = "balancer",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_arbitrum_pools_tokens_weights'),
    ref('balancer_avalanche_c_pools_tokens_weights'),
    ref('balancer_base_pools_tokens_weights'),
    ref('balancer_ethereum_pools_tokens_weights'),
    ref('balancer_gnosis_pools_tokens_weights'),
    ref('balancer_optimism_pools_tokens_weights'),
    ref('balancer_polygon_pools_tokens_weights'),
    ref('balancer_zkevm_pools_tokens_weights')
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
