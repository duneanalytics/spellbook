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
                normalized_weight
        FROM {{ ref('balancer_v2_gnosis_pools_tokens_weights') }}
)