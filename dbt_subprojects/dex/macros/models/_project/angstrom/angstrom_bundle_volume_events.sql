{% macro
    angstrom_bundle_volume_events(    
        blockchain = null,
        project = null,
        version = null
    )
%}


WITH
    tx_data AS (
        SELECT 
            block_number,
            block_time,
            hash AS tx_hash,
            index AS tx_index,
            to AS angstrom_address,
            data AS tx_data
        FROM ethereum.transactions
        WHERE to = 0xb9c4cE42C2e29132e207d29Af6a7719065Ca6AeC AND varbinary_substring(data, 1, 4) = 0x09c5eabe
    ),
    tob_orders AS (
        SELECT 
            block_number,
            block_time,
            quantity_in AS token_bought_amount_raw,
            quantity_out AS token_sold_amount_raw,
            asset_in AS token_bought_address,
            asset_out AS token_sold_address,
            recipient AS taker,
            angstrom_address AS maker,
            angstrom_address AS project_contract_address,
            tx_hash,
            row_number() OVER (PARTITION BY tx_hash) AS evt_index
        FROM {{ angstrom_bundle_tob_order_volume(tx_data) }}
    ),
    user_orders AS (
        SELECT 
            block_number,
            block_time,
            t0_amount AS token_bought_amount_raw,
            t1_amount AS token_sold_amount_raw,
            asset_in AS token_bought_address,
            asset_out AS token_sold_address,
            recipient AS taker,
            angstrom_address AS maker,
            angstrom_address AS project_contract_address,
            tx_hash,
            row_number() OVER (PARTITION BY tx_hash) + tc.tob_cnt AS evt_index
        FROM {{ angstrom_bundle_user_order_volume(tx_data, block_number) }} AS uo
        CROSS JOIN ( SELECT COUNT(*) AS tob_cnt FROM tob_orders ) AS tc
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





    -- '{{ blockchain }}' AS blockchain
    -- , '{{ project }}' AS project
    -- , '{{ version }}' AS version
    -- , CAST(date_trunc('month', block_time) AS date) AS block_month
    -- , CAST(date_trunc('day', block_time) AS date) AS block_date
    -- , block_time
    -- , block_number
    -- , token_bought_amount_raw
    -- , token_sold_amount_raw
    -- , token_bought_address
    -- , token_sold_address
    -- , taker
    -- , maker
    -- , project_contract_address
    -- , tx_hash
    -- , evt_index