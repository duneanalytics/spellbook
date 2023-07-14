{{ config(
        alias = alias('pools_tokens_weights')
        )
}}

SELECT *
FROM
(
        SELECT
                pool_id,
                token_address,
                normalized_weight
        FROM {{ ref('balancer_v1_ethereum_pools_tokens_weights') }}
        UNION
        SELECT
                pool_id,
                token_address,
                normalized_weight
        FROM {{ ref('balancer_v2_ethereum_pools_tokens_weights') }}
)