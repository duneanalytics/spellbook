{% macro
    angstrom_bundle_volume_events(   
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        earliest_block,
        blockchain,
        project = null,
        version = null
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
            p.recipient AS taker,
            p.pool_id AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            ROW_NUMBER(*) over (partition by t.tx_hash) as evt_index
        FROM tx_data_cte t
        INNER JOIN ({{ angstrom_bundle_tob_order_volume(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain) }}) AS p
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
            p.recipient AS taker,
            p.pool_id AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            ROW_NUMBER(*) over (partition by t.tx_hash) as evt_index
        FROM tx_data_cte t
        INNER JOIN ({{ angstrom_bundle_user_order_volume(angstrom_contract_addr, controller_v1_contract_addr, earliest_block, blockchain) }}) AS p 
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
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , token_bought_amount_raw
    , token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , evt_index
FROM tob_orders

UNION ALL 

SELECT 
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , token_bought_amount_raw
    , token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , evt_index
FROM user_orders


{% endmacro %}

