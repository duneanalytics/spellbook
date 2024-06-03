{{ config(
        schema = 'balancer_polygon',
        alias = 'pools_tokens_weights',
        )
}}

SELECT *
FROM
(
        SELECT
                blockchain,
                version,
                pool_id,
                token_address,
                normalized_weight
        FROM {{ ref('balancer_v2_polygon_pools_tokens_weights') }}
)