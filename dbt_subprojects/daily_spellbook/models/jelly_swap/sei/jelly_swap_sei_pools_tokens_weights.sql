{{
    config(
        schema='jelly_swap_sei',
        alias = 'pools_tokens_weights',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_id', 'token_address']
    )
}}

--
-- JellySwap Pools Tokens Weights
--
WITH registered AS (
    SELECT
        poolID AS pool_id,
        evt_block_time
    FROM {{ source('jelly_swap_sei', 'Vault_evt_PoolRegistered') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),
weighted_pool_factory AS (
    SELECT
        call_create.output_0 AS pool_id,
        t.pos AS pos,
        t.token_address AS token_address,
        t2.normalized_weight AS normalized_weight
    FROM {{ source('jelly_swap_sei', 'WeightedPoolFactory_call_create') }} AS call_create
    CROSS JOIN UNNEST(call_create.tokens) WITH ORDINALITY t(token_address, pos)
    CROSS JOIN UNNEST(call_create.normalizedWeights) WITH ORDINALITY t2(normalized_weight, pos)
    WHERE t.pos = t2.pos
    {% if is_incremental() %}
    AND {{ incremental_predicate('call_create.call_block_time') }}
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
    'sei' AS blockchain, 
    '2' AS version,
    r.pool_id, 
    w.token_address, 
    w.normalized_weight 
FROM normalized_weights w 
INNER JOIN registered r ON BYTEARRAY_SUBSTRING(r.pool_id,1,20) = w.pool_id