{{ config(
	tags=['legacy'],
	
        alias = alias('pools_tokens_weights', legacy_model=True)
        )
}}

SELECT *
FROM
(
        SELECT
                pool_id,
                token_address,
                normalized_weight
        FROM {{ ref('balancer_v2_optimism_pools_tokens_weights_legacy') }}
)