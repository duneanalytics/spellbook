{{
    config(
        alias='pools_tokens_weights',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_id', 'token_address'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
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
FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPoolFactory_call_create') }} call_create
    ON call_create.call_tx_hash = registered.evt_tx_hash
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.weights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos

UNION ALL

SELECT
    registered.`poolId` AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPool2TokensFactory_call_create') }} call_create
    ON call_create.call_tx_hash = registered.evt_tx_hash
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.weights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos

UNION ALL

SELECT
    registered.`poolId` AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPoolV2Factory_call_create') }} call_create
    ON call_create.call_tx_hash = registered.evt_tx_hash
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.normalizedWeights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos
;
