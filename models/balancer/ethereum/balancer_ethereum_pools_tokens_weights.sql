{{ config(
        alias = alias('pools_tokens_weights'),
        tags = ['dunesql']
        )
}}
 
SELECT *
FROM
(
        SELECT
                pool_id,
                token_address,
                CAST(normalized_weight as double) as normalized_weight
        FROM {{ ref('balancer_v1_ethereum_pools_tokens_weights') }}
        UNION
        SELECT
                pool_id,
                token_address,
                CAST(normalized_weight as double) as normalized_weight
        FROM {{ ref('balancer_v2_ethereum_pools_tokens_weights') }}
)
