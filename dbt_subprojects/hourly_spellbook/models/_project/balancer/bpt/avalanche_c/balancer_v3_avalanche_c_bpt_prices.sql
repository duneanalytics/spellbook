{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'bpt_prices',        
        materialized = 'table',
        file_format = 'delta'
    )
}}

-- Stub implementation for Avalanche C v3 BPT prices.
-- This model is kept to satisfy dependencies and will be
-- replaced once labels.balancer_v3_pools_avalanche_c exists
-- and full BPT pricing logic is wired for Avalanche.
WITH stub AS (
    SELECT
        CAST(NULL AS DATE)      AS day,
        'avalanche_c'           AS blockchain,
        '3'                     AS version,
        18                      AS decimals,
        CAST(NULL AS VARBINARY) AS contract_address,
        CAST(NULL AS VARCHAR)   AS pool_type,
        CAST(NULL AS DOUBLE)    AS bpt_price
    WHERE 1 = 0
)

SELECT * FROM stub