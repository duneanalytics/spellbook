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
    ),
    swap_events AS (
        SELECT
            evt_tx_hash,
            evt_index AS real_evt_index,
            id AS pool_id,
            row_number() over(partition by evt_tx_hash, id order by evt_index) AS pool_rn
        FROM {{ PoolManager_evt_Swap }}
        WHERE evt_block_number >= {{ earliest_block }}
        {%- if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {%- endif %}
    ),
    bundle_ranked AS (
        SELECT *,
            row_number() over(partition by tx_hash, maker order by evt_index) AS pool_rn
        FROM bundle_orders
    ),
    bundle_with_real_evt_index AS (
        SELECT
            b.block_time,
            b.block_number,
            b.token_bought_amount_raw,
            b.token_sold_amount_raw,
            b.token_bought_address,
            b.token_sold_address,
            b.token_sold_lp_fees_paid_raw,
            b.token_bought_lp_fees_paid_raw,
            b.token_sold_protocol_fees_paid_raw,
            b.token_bought_protocol_fees_paid_raw,
            b.taker,
            b.maker,
            b.project_contract_address,
            b.tx_hash,
            coalesce(se.real_evt_index, b.evt_index) AS evt_index,
            b.trade_type
        FROM bundle_ranked b
        LEFT JOIN swap_events se
            ON b.tx_hash = se.evt_tx_hash
            AND b.maker = se.pool_id
            AND b.pool_rn = se.pool_rn
    ),
    all_trades AS (
        SELECT * FROM composable_orders
        UNION ALL
        SELECT * FROM bundle_with_real_evt_index
    ),
    deduped AS (
        SELECT *,
            row_number() over(
                partition by tx_hash, evt_index
                order by CASE WHEN trade_type = 'Composable Swap' THEN 1 ELSE 0 END
            ) AS _rn
        FROM all_trades
    )
SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day', block_time) AS date) AS block_date,
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    token_sold_lp_fees_paid_raw,
    token_bought_lp_fees_paid_raw,
    token_sold_protocol_fees_paid_raw,
    token_bought_protocol_fees_paid_raw,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index,
    trade_type
FROM deduped
WHERE _rn = 1


{% endmacro %}

