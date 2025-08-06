{% macro
    angstrom_bundle_volume_events(   
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        blockchain,
        project = null,
        version = null
    )
%}

-------------------- TO TEST ------------------

-- single, TOB: 23077861 - 0xb72c702151c9004f3f327a82cfe451f69a206c21b82fa98419791ebc0bc29b94
-- single, USER: 23077829 - 0x32716081b3461e4f4770e14d97565c003aecf647837d151a8380f6b9722e7faf
-- multi, TOB: 
    -- 23085211 - 0xbb0cb5d7062a838a9b590a202a6e9b6478aa7e9a78824a21576dae1662b7dbcb
    -- 23085199 - 0xf07e41f652e68359a2c2fa1e571fdd05fa0eb4430da3941ce96744ac873408b1
    -- 23085183 - 0x627d33d7a00554446b2e4d109bc695c5d5b1131ed68980a24250e36103102c89
-- multi, USER: 
    -- 23084306 - 0x5f0a2eb5ea030dc3f18d03901ffe4ec161bb5fb5942e9904a3d1a75d5e6e53cc
    -- 23084299 - 0xd46f57a0e3aaa61a5f711cd7d2cf90f083e7e37d9125dd07e300a27d554c9c46
    -- 23083864 - 0x6e299e112769472208e63bd05bf40787ff9168c4731c6daa601c25b67f125d95

-----------------------------------------------



WITH
    tx_data_cte AS (
        {{ angstrom_tx_data(angstrom_contract_addr, blockchain) }}
    ),
    tob_orders AS (
        SELECT 
            t.block_number AS block_number,
            t.block_time AS block_time,
            p.quantity_in       AS token_bought_amount_raw,
            p.quantity_out      AS token_sold_amount_raw,
            p.asset_out          AS token_bought_address,
            p.asset_in         AS token_sold_address,
            p.recipient AS taker,
            t.angstrom_address AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            row_number() over (partition by t.tx_hash) as evt_index
        FROM tx_data_cte t
        INNER JOIN ({{ angstrom_bundle_tob_order_volume(angstrom_contract_addr, blockchain) }}) AS p
            ON t.tx_hash = p.tx_hash AND t.block_number = p.block_number
    ),
    user_orders AS (
        SELECT 
            t.block_number AS block_number,
            t.block_time AS block_time,
            p.t0_amount AS token_bought_amount_raw,
            p.t1_amount AS token_sold_amount_raw,
            p.asset_out AS token_bought_address,
            p.asset_in AS token_sold_address,
            p.recipient AS taker,
            t.angstrom_address AS maker,
            t.angstrom_address AS project_contract_address,
            t.tx_hash AS tx_hash,
            row_number() OVER (PARTITION BY t.tx_hash) + tc.tob_cnt AS evt_index
        FROM tx_data_cte t
        INNER JOIN ({{ angstrom_bundle_user_order_volume(angstrom_contract_addr, controller_v1_contract_addr, blockchain) }}) AS p 
            ON t.tx_hash = p.tx_hash AND t.block_number = p.block_number
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



{% endmacro %}
