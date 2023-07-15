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
        FROM {{ ref('balancer_v2_arbitrum_pools_tokens_weights') }}
)