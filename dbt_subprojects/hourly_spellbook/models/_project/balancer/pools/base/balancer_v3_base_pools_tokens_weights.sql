{{
    config(
        schema='balancer_v3_base',
        alias = 'pools_tokens_weights',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_id', 'token_address']
    )
}}

--
-- Balancer v3 Pools Tokens Weights
--
WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(FROM_HEX(json_extract_scalar(token, '$.token')) ORDER BY token_index) AS tokens 
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM {{ source('balancer_v3_base', 'Vault_evt_PoolRegistered') }}
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
    FROM {{ source('balancer_v3_base', 'WeightedPoolFactory_call_create') }} AS call_create
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
    'base' AS blockchain, 
    '3' AS version,
    w.pool_id, 
    w.token_address, 
    w.normalized_weight 
FROM normalized_weights w 
WHERE w.pool_id IS NOT NULL
