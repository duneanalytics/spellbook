{% macro
    angstrom_bundle_user_orders(   
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        earliest_block,
        blockchain,
        controller_pool_configured_log_topic0,
        bundle_tob_orders_table = none,
        user_orders_decoding_table = none,
        tob_orders_decoding_table = none,
        pool_updates_decoding_table = none,
        assets_decoding_table = none,
        pairs_decoding_table = none
    )
%}


WITH
    tx_data_cte AS (
        {{ angstrom_tx_data(angstrom_contract_addr, earliest_block, blockchain) }}
    ),
    tob_counts AS (
        SELECT
            t.tx_hash AS tx_hash,
            COUNT(*) AS tob_cnt
        FROM tx_data_cte t
        INNER JOIN (
            {% if bundle_tob_orders_table is not none %}
            SELECT
                tx_hash,
                block_number
            FROM {{ bundle_tob_orders_table }}
            {% else %}
            {{
                angstrom_bundle_tob_order_volume(
                    angstrom_contract_addr, 
                    controller_v1_contract_addr,
                    earliest_block,
                    blockchain,
                    controller_pool_configured_log_topic0,
                    tob_orders_decoding_table,
                    pool_updates_decoding_table,
                    user_orders_decoding_table,
                    assets_decoding_table,
                    pairs_decoding_table
                )
            }}
            {% endif %}
        ) AS p
            ON t.tx_hash = p.tx_hash
            AND t.block_number = p.block_number
        GROUP BY tx_hash
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
            ROW_NUMBER(*) over (partition by t.tx_hash order by p.pool_id, p.recipient, p.asset_in, p.asset_out, p.token_sold_amt, p.token_bought_amt, p.lp_fees_paid_asset_in, p.lp_fees_paid_asset_out, p.protocol_fees_paid_asset_in, p.protocol_fees_paid_asset_out) as evt_index
        FROM tx_data_cte t
        INNER JOIN (
            {{
                angstrom_bundle_user_order_volume(
                    angstrom_contract_addr,
                    controller_v1_contract_addr,
                    earliest_block,
                    blockchain,
                    controller_pool_configured_log_topic0,
                    user_orders_decoding_table,
                    assets_decoding_table,
                    pairs_decoding_table
                )
            }}
        ) AS p 
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
        LEFT JOIN tob_counts AS tc
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
    'User Order' AS trade_type
FROM user_orders


{% endmacro %}
