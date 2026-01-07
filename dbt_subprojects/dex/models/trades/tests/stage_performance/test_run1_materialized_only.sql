{{
    config(
        schema = 'dex',
        alias = 'test_run1_materialized_only',
        materialized = 'table',
        tags = ['performance_test']
    )
}}

-- Run 1: Materialized dex.trades only
-- Query Structure: FROM dex.trades → JOIN dex.trades
-- Expected: 63 stages, 52.51s elapsed, 10.01m CPU, 9.53GB memory

WITH trades AS (
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
    FROM {{ ref('dex_trades') }}
    WHERE blockchain = 'ethereum'
        AND amount_usd > 0
        AND project_contract_address != 0x000000000004444c5dc75cb358380d2e3de08a90
        AND project != '1inch-LOP'
        AND project != '0x-API'
        AND token_bought_amount > 0
        AND token_sold_amount > 0
        AND block_time >= CURRENT_DATE - INTERVAL '24' HOUR
        AND block_time <= CURRENT_DATE
)

, pool_details AS (
    SELECT
        l.*
        , SUM(dt.amount_usd) AS trade_volume_usd_24h
    FROM trades l
    JOIN {{ ref('dex_trades') }} dt
        ON l.project_contract_address = dt.project_contract_address
    WHERE dt.block_time >= CURRENT_DATE - INTERVAL '24' HOUR
        AND dt.block_time <= CURRENT_DATE
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
)

SELECT * FROM pool_details

