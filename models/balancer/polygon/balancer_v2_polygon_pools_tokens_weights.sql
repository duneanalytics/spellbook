{{
    config(
        schema='balancer_v2_polygon',
        alias = 'pools_tokens_weights',
        
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_id', 'token_address']
    )
}}

--
-- Balancer v2 Pools Tokens Weights
--
WITH registered AS (
    SELECT
        poolID AS pool_id,
        evt_block_time
    FROM {{ source('balancer_v2_polygon', 'Vault_evt_PoolRegistered') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),
weighted_pool_factory AS (
    SELECT
        call_create.output_0 AS pool_id,
        t.pos AS pos,
        t.token_address AS token_address,
        t2.normalized_weight AS normalized_weight
    FROM {{ source('balancer_v2_polygon', 'WeightedPoolFactory_call_create') }} AS call_create
    CROSS JOIN UNNEST(call_create.tokens) WITH ORDINALITY t(token_address, pos)
    CROSS JOIN UNNEST(call_create.weights) WITH ORDINALITY t2(normalized_weight, pos)
    WHERE t.pos = t2.pos
    {% if is_incremental() %}
    AND call_create.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL

    SELECT
        call_create.output_0 AS pool_id,
        t.pos AS pos,
        t.token_address AS token_address,
        t2.normalized_weight AS normalized_weight
    FROM {{ source('balancer_v2_polygon', 'WeightedPoolFactory_call_create') }} AS call_create
    CROSS JOIN UNNEST(call_create.tokens) WITH ORDINALITY t(token_address, pos)
    CROSS JOIN UNNEST(call_create.normalizedWeights) WITH ORDINALITY t2(normalized_weight, pos)
    WHERE t.pos = t2.pos
    {% if is_incremental() %}
    AND call_create.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),
weighted_pool_2tokens_factory AS (
    SELECT
        call_create.output_0 AS pool_id,
        t.pos AS pos,
        t.token_address AS token_address,
        t2.normalized_weight AS normalized_weight
    FROM {{ source('balancer_v2_polygon', 'WeightedPool2TokensFactory_call_create') }} AS call_create
    CROSS JOIN UNNEST(call_create.tokens) WITH ORDINALITY t(token_address, pos)
    CROSS JOIN UNNEST(call_create.weights) WITH ORDINALITY t2(normalized_weight, pos)
    WHERE t.pos = t2.pos
    {% if is_incremental() %}
    AND call_create.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),
weighted_pool_v2_factory AS (
    SELECT
        call_create.output_0 AS pool_id,
        t.pos AS pos,
        t.token_address AS token_address,
        t2.normalized_weight AS normalized_weight
    FROM {{ source('balancer_v2_polygon', 'WeightedPoolV2Factory_call_create') }} AS call_create
    CROSS JOIN UNNEST(call_create.tokens) WITH ORDINALITY t(token_address, pos)
    CROSS JOIN UNNEST(call_create.normalizedWeights) WITH ORDINALITY t2(normalized_weight, pos)
    WHERE t.pos = t2.pos
    {% if is_incremental() %}
    AND call_create.call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),
normalized_weights AS (
    SELECT
        pool_id,
        token_address,
        normalized_weight / POWER(10, 18) AS normalized_weight
    FROM weighted_pool_factory
    UNION ALL
    SELECT
        pool_id,
        token_address,
        normalized_weight / POWER(10, 18) AS normalized_weight
    FROM weighted_pool_2tokens_factory
    UNION ALL
    SELECT
        pool_id,
        token_address,
        normalized_weight / POWER(10, 18) AS normalized_weight
    FROM weighted_pool_v2_factory
)

SELECT 
    'polygon' AS blockchain, 
    '2' AS version,
    r.pool_id, 
    w.token_address, 
    w.normalized_weight 
FROM normalized_weights w 
LEFT JOIN registered r ON BYTEARRAY_SUBSTRING(r.pool_id,1,20) = w.pool_id
WHERE w.pool_id IS NOT NULL
