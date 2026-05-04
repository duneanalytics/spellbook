{{ config(
        schema = 'balancer_plasma',
        alias = 'pools_tokens_weights',
        
        )
}}

{% set balancer_models = [
    ref('balancer_v3_plasma_pools_tokens_weights')
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
;
