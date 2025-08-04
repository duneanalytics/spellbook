{% macro
    angstrom_bundle_volume_events(   
        angstrom_contract_addr, 
        blockchain,
        project = null,
        version = null
    )
%}


WITH
    tx_data_cte AS (
        SELECT 
            block_number,
            block_time,
            hash AS tx_hash,
            index AS tx_index,
            to AS angstrom_address,
            data AS tx_data
        FROM {{ source(blockchain, 'transactions') }}
        WHERE to = {{ angstrom_contract_addr }} AND varbinary_substring(data, 1, 4) = 0x09c5eabe AND hash = 0x47aefe13a19c8036c0985b59090a34adffcad108630a86aae298954554394d10
    ),
    tob_orders AS (
        SELECT 
            t.block_number AS block_number,
            t.block_time AS block_time,
            p.quantity_in       AS token_bought_amount_raw,
            p.quantity_out      AS token_sold_amount_raw,
            p.asset_in          AS token_bought_address,
            p.asset_out         AS token_sold_address,
            p.recipient AS taker,
            t.angstrom_address AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            row_number() over (partition by t.tx_hash) as evt_index
        FROM tx_data_cte t
        CROSS JOIN LATERAL ({{ angstrom_bundle_tob_order_volume('t.tx_data') }}) AS p
    ),
    user_orders AS (
        SELECT 
            t.block_number AS block_number,
            t.block_time AS block_time,
            p.t0_amount AS token_bought_amount_raw,
            p.t1_amount AS token_sold_amount_raw,
            p.asset_in AS token_bought_address,
            p.asset_out AS token_sold_address,
            p.recipient AS taker,
            t.angstrom_address AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            row_number() OVER (PARTITION BY t.tx_hash) + tc.tob_cnt AS evt_index
        FROM tx_data_cte t
        CROSS JOIN ( SELECT COUNT(*) AS tob_cnt FROM tob_orders ) AS tc
        CROSS JOIN LATERAL ({{ angstrom_bundle_user_order_volume(angstrom_contract_addr, blockchain, 't.tx_data', 't.block_number') }}) AS p

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
