{% macro
    angstrom_bundle_orders(   
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        earliest_block,
        blockchain,
        controller_pool_configured_log_topic0
    )
%}


WITH
    tx_data_cte AS (
        {{ angstrom_tx_data(angstrom_contract_addr, earliest_block, blockchain) }}
    ),
    tob_orders AS (
        SELECT 
            t.block_number AS block_number,
            t.block_time AS block_time,
            p.quantity_out       AS token_bought_amount_raw,
            p.quantity_in      AS token_sold_amount_raw,
            p.asset_out          AS token_bought_address,
            p.asset_in         AS token_sold_address,
            p.fees_paid_asset_in AS token_sold_lp_fees_paid_raw,
            p.fees_paid_asset_out AS token_bought_lp_fees_paid_raw,
            0 AS token_sold_protocol_fees_paid_raw,
            0 AS token_bought_protocol_fees_paid_raw,
            p.recipient AS taker,
            p.pool_id AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            ROW_NUMBER(*) over (partition by t.tx_hash) as evt_index
        FROM tx_data_cte t
        INNER JOIN ({{ angstrom_bundle_tob_order_volume(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0) }}) AS p
            ON t.tx_hash = p.tx_hash AND t.block_number = p.block_number
    ),
    user_orders_inner AS (
        SELECT 
            t.block_number AS block_number,
            t.block_time AS block_time,
            p.token_bought_amt AS token_bought_amount_raw,
            p.token_sold_amt AS token_sold_amount_raw,
            p.asset_out AS token_bought_address,
            p.asset_in AS token_sold_address,
            p.lp_fees_paid_asset_in AS token_sold_lp_fees_paid_raw,
            p.lp_fees_paid_asset_out AS token_bought_lp_fees_paid_raw,
            p.protocol_fees_paid_asset_in AS token_sold_protocol_fees_paid_raw,
            p.protocol_fees_paid_asset_out AS token_bought_protocol_fees_paid_raw,
            p.recipient AS taker,
            p.pool_id AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            ROW_NUMBER(*) over (partition by t.tx_hash) as evt_index
        FROM tx_data_cte t
        INNER JOIN ({{ angstrom_bundle_user_order_volume(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain, controller_pool_configured_log_topic0) }}) AS p 
            ON t.tx_hash = p.tx_hash AND t.block_number = p.block_number
    ),
    user_orders AS (
        SELECT
            t.block_number AS block_number,
            t.block_time AS block_time,
            t.token_bought_amount_raw AS token_bought_amount_raw,
            t.token_sold_amount_raw AS token_sold_amount_raw,
            t.token_bought_address AS token_bought_address,
            t.token_sold_address AS token_sold_address,
            t.token_sold_lp_fees_paid_raw AS token_sold_lp_fees_paid_raw,
            t.token_bought_lp_fees_paid_raw AS token_bought_lp_fees_paid_raw,
            t.token_sold_protocol_fees_paid_raw AS token_sold_protocol_fees_paid_raw,
            t.token_bought_protocol_fees_paid_raw AS token_bought_protocol_fees_paid_raw,
            t.taker AS taker,
            t.maker AS maker,
            t.project_contract_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            t.evt_index + coalesce(tc.tob_cnt, 0) AS evt_index
        FROM user_orders_inner AS t
        LEFT JOIN ( 
            SELECT 
                tx_hash,
                COUNT(*) AS tob_cnt 
            FROM tob_orders 
            GROUP BY tx_hash
        ) AS tc
        ON tc.tx_hash = t.tx_hash
    )
    
SELECT
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
    'Top Of Block Order' AS trade_type
FROM tob_orders

UNION ALL 

SELECT 
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
    'User Order' AS trade_type
FROM user_orders


{% endmacro %}

