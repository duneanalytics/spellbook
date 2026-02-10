{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta'
    )
}}

-- Stub implementation for Avalanche C v3 liquidity.
-- This model is kept to satisfy dependencies and will be
-- replaced once labels.balancer_v3_pools_avalanche_c exists
-- and full liquidity logic is wired for Avalanche.
WITH stub AS (
    SELECT
        CAST(NULL AS DATE)      AS day,
        CAST(NULL AS VARBINARY) AS pool_id,
        CAST(NULL AS VARBINARY) AS pool_address,
        CAST(NULL AS VARCHAR)   AS pool_symbol,
        '3'                     AS version,
        'avalanche_c'           AS blockchain,
        CAST(NULL AS VARCHAR)   AS pool_type,
        CAST(NULL AS VARBINARY) AS token_address,
        CAST(NULL AS VARCHAR)   AS token_symbol,
        CAST(NULL AS BIGINT)    AS token_balance_raw,
        CAST(NULL AS DOUBLE)    AS token_balance,
        CAST(NULL AS DOUBLE)    AS protocol_liquidity_usd,
        CAST(NULL AS DOUBLE)    AS protocol_liquidity_eth,
        CAST(NULL AS DOUBLE)    AS pool_liquidity_usd,
        CAST(NULL AS DOUBLE)    AS pool_liquidity_eth
    WHERE 1 = 0
)

SELECT * FROM stub