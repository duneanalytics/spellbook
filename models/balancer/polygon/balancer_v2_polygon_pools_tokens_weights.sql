{{
    config(
        schema='balancer_v2_polygon',
        alias = alias('pools_tokens_weights'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_id', 'token_address'],
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["metacrypto", "jacektrocinski"]\') }}'
    )
}}

--
-- Balancer v2 Pools Tokens Weights
--
SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_polygon', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_polygon', 'WeightedPoolFactory_call_create') }} call_create
    ON call_create.call_tx_hash = registered.evt_tx_hash
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.weights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos
    {% if is_incremental() %}
    AND registered.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
UNION ALL

SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_polygon', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_polygon', 'WeightedPool2TokensFactory_call_create') }} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.weights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos
    {% if is_incremental() %}
    AND registered.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
UNION ALL

SELECT
    registered.poolId AS pool_id,
    tokens.token_address,
    weights.normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_polygon', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_polygon', 'WeightedPoolV2Factory_call_create') }} call_create
    ON call_create.output_0 = SUBSTRING(registered.poolId, 0, 42)
    LATERAL VIEW posexplode(call_create.tokens) tokens AS pos, token_address
    LATERAL VIEW posexplode(call_create.normalizedWeights) weights AS pos, normalized_weight
WHERE tokens.pos = weights.pos
    {% if is_incremental() %}
    AND registered.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
;
