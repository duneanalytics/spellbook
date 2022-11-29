{{
    config(
        schema = 'balancer_v2_ethereum',
        alias='pools_tokens_weights',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer",
                                    \'["metacrypto", "jacektrocinski"]\') }}'
    ) 
}}

--
-- Balancer v2 Pools Tokens Weights
--
SELECT
    registered.`poolId` AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_ethereum', 'WeightedPoolFactory_call_create') }} call_create
    ON call_create.call_tx_hash = registered.evt_tx_hash
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.weights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos

UNION ALL

SELECT
    registered.`poolId` AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_ethereum', 'WeightedPool2TokensFactory_call_create') }} call_create
    ON call_create.call_tx_hash = registered.evt_tx_hash
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.weights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos

UNION ALL

SELECT
    registered.`poolId` AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_ethereum', 'WeightedPoolV2Factory_call_create') }} call_create
    ON call_create.call_tx_hash = registered.evt_tx_hash
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.normalizedWeights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos
;
