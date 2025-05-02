{{ 
    config(
    schema = 'pancakeswap_infinity_bnb'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH

dexs_clamm AS (
    -- PancakeSwap Infinity CLAMM (Concentrated Liquidity AMM)
    {{
        pancakeswap_compatible_infinity_cl_trades(
            blockchain = 'bnb'
            , project = 'pancakeswap'
            , version = 'infinity_cl'
            , PoolManager_call_Swap = source('pancakeswap_infinity_bnb', 'ClPoolManager_call_Swap') 
            , PoolManager_evt_Swap = source('pancakeswap_infinity_bnb', 'ClPoolManager_evt_Swap') 
        )
    }}
), 
dexs_lbamm AS (
    -- PancakeSwap Infinity LBAMM (Liquidity Book AMM)
    {{
        pancakeswap_compatible_infinity_lb_trades(
            blockchain = 'bnb'
            , project = 'pancakeswap'
            , version = 'infinity_lb'
            , PoolManager_call_Swap = source('pancakeswap_infinity_bnb', 'BinPoolManager_call_Swap') 
            , PoolManager_evt_Swap = source('pancakeswap_infinity_bnb', 'BinPoolManager_evt_Swap') 
        )
    }}
)


SELECT
    dexs_clamm.blockchain,
    dexs_clamm.project,
    dexs_clamm.version,
    dexs_clamm.block_month,
    dexs_clamm.block_date,
    dexs_clamm.block_time,
    dexs_clamm.block_number,
    dexs_clamm.token_bought_amount_raw,
    dexs_clamm.token_sold_amount_raw,
    dexs_clamm.token_bought_address,
    dexs_clamm.token_sold_address,
    dexs_clamm.taker,
    dexs_clamm.maker,
    dexs_clamm.project_contract_address,
    dexs_clamm.tx_hash,
    dexs_clamm.evt_index
FROM dexs_clamm

UNION ALL

SELECT
    dexs_lbamm.blockchain,
    dexs_lbamm.project,
    dexs_lbamm.version,
    dexs_lbamm.block_month,
    dexs_lbamm.block_date,
    dexs_lbamm.block_time,
    dexs_lbamm.block_number,
    dexs_lbamm.token_bought_amount_raw,
    dexs_lbamm.token_sold_amount_raw,
    dexs_lbamm.token_bought_address,
    dexs_lbamm.token_sold_address,
    dexs_lbamm.taker,
    dexs_lbamm.maker,
    dexs_lbamm.project_contract_address,
    dexs_lbamm.tx_hash,
    dexs_lbamm.evt_index
FROM dexs_lbamm