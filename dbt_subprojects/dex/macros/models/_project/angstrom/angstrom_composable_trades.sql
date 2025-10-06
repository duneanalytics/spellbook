{% macro
    angstrom_composable_trades(   
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        earliest_block,
        blockchain,
        controller_pool_configured_log_topic0,
        PoolManager_call_Swap = null,
        PoolManager_evt_Swap = null,
        taker_column_name = null
    )
%}




WITH
    baseline_swaps AS (
        SELECT 
            *
        FROM ({{ angstrom_composable_uni_v4_events(angstrom_contract_addr, earliest_block, blockchain, PoolManager_call_Swap, PoolManager_evt_Swap, taker_column_name) }}) AS u
    ),
    pool_info_with_lp_fee AS (
        SELECT
            b.*,
            i.protocol_unlocked_fee,
            ROUND(b.token_sold_amount_raw * i.unlocked_fee / 1e6) AS token_sold_lp_fees_paid_raw,
            if(b.zero_for_one != (b.amount_specified < 0), i.token0, i.token1) AS protocol_fee_paid_token
        FROM baseline_swaps AS b
        INNER JOIN ( {{ angstrom_pool_info(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0) }} ) AS i
            ON b.block_number = i.block_number AND b.maker = i.pool_id
    ),
    protocol_and_lp_fees AS (
        SELECT 
            p.*,
            f.fee_amount AS protocol_fee_amount
        FROM pool_info_with_lp_fee AS p
        CROSS JOIN LATERAL ({{ angstrom_composable_protocol_fee_calc('p.protocol_unlocked_fee', 'p.zero_for_one', 'p.amount_specified', 'p.token_amount0', 'p.token_amount1') }}) AS f

    )
SELECT 
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    token_sold_lp_fees_paid_raw,
    0 AS token_bought_lp_fees_paid_raw,
    if(protocol_fee_paid_token = token_sold_address, protocol_fee_amount, 0) AS token_sold_protocol_fees_paid_raw,
    if(protocol_fee_paid_token = token_bought_address, protocol_fee_amount, 0) AS token_bought_protocol_fees_paid_raw,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index,
    'Composable Swap' AS trade_type
FROM protocol_and_lp_fees


{% endmacro %}
