{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'bpt_supply',
        materialized = 'table',
        file_format = 'delta'

    )
}}

-- Stub implementation for Avalanche C v3 BPT supply.
-- This model is kept to satisfy dependencies and will be
-- replaced once labels.balancer_v3_pools_avalanche_c exists
-- and the v3 BPT logic is fully wired for Avalanche.
WITH stub AS (
    SELECT
        CAST(NULL AS DATE)      AS day,
        CAST(NULL AS VARCHAR)   AS pool_type,
        '3'                     AS version,
        'avalanche_c'           AS blockchain,
        CAST(NULL AS VARBINARY) AS token_address,
        CAST(NULL AS DOUBLE)    AS supply
    WHERE 1 = 0
)

SELECT * FROM stub