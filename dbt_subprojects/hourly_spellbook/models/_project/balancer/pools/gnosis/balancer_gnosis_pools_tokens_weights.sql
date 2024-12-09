{{ config(
        schema = 'balancer_gnosis',
        alias = 'pools_tokens_weights',
        
        )
}}

{% set balancer_models = [
    ref('balancer_v2_gnosis_pools_tokens_weights'),
    ref('balancer_v3_gnosis_pools_tokens_weights')
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
    WHERE pool_id IS NOT NULL
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
