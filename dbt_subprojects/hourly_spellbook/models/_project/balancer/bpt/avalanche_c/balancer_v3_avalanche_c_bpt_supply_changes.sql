{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'bpt_supply_changes', 
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index', 'label'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

-- Stub implementation for Avalanche C v3 BPT supply changes.
-- This model is kept to satisfy dependencies and will be
-- replaced once labels.balancer_v3_pools_avalanche_c exists
-- and full BPT supply-change logic is wired for Avalanche.
WITH stub AS (
    SELECT
        CAST(NULL AS DATE)      AS block_date,
        CAST(NULL AS TIMESTAMP) AS evt_block_time,
        CAST(NULL AS BIGINT)    AS evt_block_number,
        'avalanche_c'           AS blockchain,
        CAST(NULL AS VARBINARY) AS evt_tx_hash,
        CAST(NULL AS INTEGER)   AS evt_index,
        CAST(NULL AS VARCHAR)   AS pool_type,
        CAST(NULL AS VARCHAR)   AS pool_symbol,
        '3'                     AS version,
        CAST(NULL AS VARCHAR)   AS label,
        CAST(NULL AS VARBINARY) AS token_address,
        CAST(NULL AS BIGINT)    AS delta_amount_raw,
        CAST(NULL AS DOUBLE)    AS delta_amount
    WHERE 1 = 0
)

SELECT * FROM stub