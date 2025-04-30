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
    -- PancakeSwap Infinity CLAMM
    {{
        pancakeswap_compatible_v4_trades(
            blockchain = 'bnb'
            , project = 'pancakeswap'
            , version = 'infinity_cl'
            , PoolManager_call_Swap = source('pancakeswap_infinity_bnb', 'ClPoolManager_call_Swap') 
            , PoolManager_evt_Swap = source('pancakeswap_infinity_bnb', 'ClPoolManager_evt_Swap') 
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