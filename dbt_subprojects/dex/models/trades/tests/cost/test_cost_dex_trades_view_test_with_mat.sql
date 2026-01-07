{{
    config(
        schema = 'dex',
        alias = 'test_cost_dex_trades_view_test_with_mat',
        materialized = 'table',
        tags = ['cost_test']
    )
}}

-- Cost Test: Using dex.trades_view_test_with_mat_only (view with materialized as_is DEXes)
-- This test measures the cost of querying dex.trades_view_test_with_mat_only for staking asset analysis
-- Reference: https://dune.com/queries/6244496
-- Note: This version uses materialized as_is DEXes (oneinch_lop_own_trades_mat, zeroex_native_trades_mat)
-- Expected: Similar cost to dex.trades_view_test as materialization of as_is components doesn't significantly impact cost

SELECT 
    d.blockchain
    , d.project
    , d.version
    , d.block_time
    , d.block_number
    , d.evt_index
    , d.tx_hash
    , d.maker
    , d.block_date
    , d.project_contract_address
    , d.token_pair
    , d.token_bought_address
    , d.token_sold_address
    , d.amount_usd
    , d.token_bought_amount_raw
    , d.token_sold_amount_raw
    , d.token_bought_amount
    , d.token_sold_amount
    , 'token_bought' AS type_
    , d.token_bought_symbol
    , d.token_sold_symbol
    , tsa.token_decimals
    , tsa.token_symbol
    , tsa.secondary_trait AS token_type
    , CAST(NOW() AS TIMESTAMP) AS last_updated
FROM {{ ref('dex_trades_view_test_with_mat_only') }} d 
INNER JOIN {{ source('ether_fi', 'result_traits_staking_assets') }} tsa 
    ON d.blockchain = tsa.blockchain 
    AND d.token_bought_address = tsa.token_address 
WHERE d.block_date >= DATE '2025-01-01'
    AND INITCAP(d.project) != '1inch Lop'

UNION ALL 

SELECT 
    d.blockchain
    , d.project
    , d.version
    , d.block_time
    , d.block_number
    , d.evt_index
    , d.tx_hash
    , d.maker
    , d.block_date
    , d.project_contract_address
    , d.token_pair
    , d.token_bought_address
    , d.token_sold_address
    , d.amount_usd
    , d.token_bought_amount_raw
    , d.token_sold_amount_raw
    , d.token_bought_amount
    , d.token_sold_amount
    , 'token_sold' AS type_
    , d.token_bought_symbol
    , d.token_sold_symbol
    , tsa.token_decimals
    , tsa.token_symbol
    , tsa.secondary_trait AS token_type
    , CAST(NOW() AS TIMESTAMP) AS last_updated
FROM {{ ref('dex_trades_view_test_with_mat_only') }} d 
INNER JOIN {{ source('ether_fi', 'result_traits_staking_assets') }} tsa 
    ON d.blockchain = tsa.blockchain 
    AND d.token_sold_address = tsa.token_address 
WHERE d.block_date >= DATE '2025-01-01'
    AND INITCAP(d.project) != '1inch Lop'

