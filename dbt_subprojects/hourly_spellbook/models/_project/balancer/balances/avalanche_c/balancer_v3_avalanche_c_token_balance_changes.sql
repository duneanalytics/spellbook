{% set blockchain = 'avalanche_c' %}

{{ config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'token_balance_changes',
        materialized = 'table',
        file_format = 'delta'
    )
}}

-- Stub implementation for Avalanche C v3 token balance changes.
-- This model is kept to satisfy dependencies (e.g. daily agg models)
-- and will be replaced once labels.balancer_v3_pools_avalanche_c
-- and the underlying v3 contracts are fully wired.
WITH stub AS (
    SELECT
        CAST(NULL AS TIMESTAMP) AS block_date,
        CAST(NULL AS TIMESTAMP) AS evt_block_time,
        CAST(NULL AS BIGINT)    AS evt_block_number,
        'avalanche_c'           AS blockchain,
        CAST(NULL AS VARBINARY) AS evt_tx_hash,
        CAST(NULL AS INTEGER)   AS evt_index,
        CAST(NULL AS VARBINARY) AS pool_id,
        CAST(NULL AS VARBINARY) AS pool_address,
        CAST(NULL AS VARCHAR)   AS pool_symbol,
        CAST(NULL AS VARCHAR)   AS pool_type,
        '3'                     AS version,
        CAST(NULL AS VARBINARY) AS token_address,
        CAST(NULL AS VARCHAR)   AS token_symbol,
        CAST(NULL AS INT256)    AS delta_amount_raw,
        CAST(NULL AS DOUBLE)    AS delta_amount
    WHERE 1 = 0
)

SELECT * FROM stub