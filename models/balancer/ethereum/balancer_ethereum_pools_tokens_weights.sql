{{ config(
        schema = 'balancer_ethereum',
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
                CAST(normalized_weight as double) as normalized_weight
        FROM {{ ref('balancer_v1_ethereum_pools_tokens_weights') }}
        UNION
        SELECT
                blockchain,
                version,
                pool_id,
                token_address,
                CAST(normalized_weight as double) as normalized_weight
        FROM {{ ref('balancer_v2_ethereum_pools_tokens_weights') }}
)
