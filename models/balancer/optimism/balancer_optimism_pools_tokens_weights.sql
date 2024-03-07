{{ config(
        schema = 'balancer_optimism',
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
        FROM {{ ref('balancer_v2_optimism_pools_tokens_weights') }}
)