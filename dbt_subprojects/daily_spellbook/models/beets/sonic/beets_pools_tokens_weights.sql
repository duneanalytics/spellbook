{{
    config(
        schema='beets',
        alias = 'pools_tokens_weights',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_id', 'token_address']
    )
}}

WITH v2 AS(
WITH registered AS (
    SELECT
        poolID AS pool_id,
        evt_block_time
    FROM {{ source('beethoven_x_v2_sonic', 'Vault_evt_PoolRegistered') }}
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
    FROM {{ source('beethoven_x_v2_sonic', 'WeightedPoolFactory_call_create') }} AS call_create
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
)

SELECT 
    'sonic' AS blockchain, 
    '2' AS version,
    r.pool_id, 
    w.token_address, 
    w.normalized_weight 
FROM normalized_weights w 
LEFT JOIN registered r ON BYTEARRAY_SUBSTRING(r.pool_id,1,20) = w.pool_id
WHERE w.pool_id IS NOT NULL),

v3 AS(
WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(FROM_HEX(json_extract_scalar(token, '$.token')) ORDER BY token_index) AS tokens 
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM {{ source('beethoven_x_v3_sonic', 'Vault_evt_PoolRegistered') }}
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1
    ),

weighted_pool_factory AS (
    SELECT
        call_create.output_pool AS pool_id,
        t.pos AS pos,
        t.token_address AS token_address,
        t2.normalized_weight AS normalized_weight
    FROM {{ source('beethoven_x_v3_sonic', 'WeightedPoolFactory_call_create') }} AS call_create
    JOIN token_data td ON td.pool = call_create.output_pool
    CROSS JOIN UNNEST(td.tokens) WITH ORDINALITY t(token_address, pos)
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
)

SELECT 
    'sonic' AS blockchain, 
    '3' AS version,
    w.pool_id, 
    w.token_address, 
    w.normalized_weight 
FROM normalized_weights w 
WHERE w.pool_id IS NOT NULL
)

SELECT * FROM v2
UNION
SELECT * FROM v3