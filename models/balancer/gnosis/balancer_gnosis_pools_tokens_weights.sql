{{ config(
        schema = 'balancer_gnosis',
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
        FROM {{ ref('balancer_v2_gnosis_pools_tokens_weights') }}
)