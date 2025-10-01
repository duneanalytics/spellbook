{% macro
    angstrom_all_trades(   
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        earliest_block,
        blockchain,
        controller_pool_configured_log_topic0,
        project = null,
        version = null,
        PoolManager_call_Swap = null,
        PoolManager_evt_Swap = null,
        taker_column_name = null
    )
%}


WITH
    bundle_orders AS (
        SELECT * 
        FROM ({{ angstrom_bundle_orders(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0) }})
    ),
    composable_orders AS (
        SELECT *
        FROM ({{ angstrom_composable_trades(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0, PoolManager_call_Swap, PoolManager_evt_Swap, taker_column_name) }})
    )
SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day', block_time) AS date) AS block_date,
    *
FROM composable_orders

UNION ALL 

SELECT 
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day', block_time) AS date) AS block_date,
    *
FROM bundle_orders


{% endmacro %}

