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
        taker_column_name = null,
        bundle_orders_table = none,
        bundle_tob_orders_table = none,
        bundle_user_orders_table = none,
        composable_orders_table = none
    )
%}


WITH
    bundle_orders AS (
        {% if bundle_orders_table is not none %}
        SELECT *
        FROM {{ bundle_orders_table }}
        {% elif bundle_tob_orders_table is not none and bundle_user_orders_table is not none %}
        SELECT *
        FROM {{ bundle_tob_orders_table }}
        UNION ALL
        SELECT *
        FROM {{ bundle_user_orders_table }}
        {% else %}
        SELECT * 
        FROM ({{ angstrom_bundle_orders(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0) }})
        {% endif %}
    ),
    composable_orders AS (
        {% if composable_orders_table is not none %}
        SELECT *
        FROM {{ composable_orders_table }}
        {% else %}
        SELECT *
        FROM ({{ angstrom_composable_trades(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0, PoolManager_call_Swap, PoolManager_evt_Swap, taker_column_name) }})
        {% endif %}
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
