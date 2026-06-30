{{ config(
    schema = 'pancakeswap_infinity_cl_base'
    , alias = 'sqrtpricex96_latest'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'id']
    )
}}

{{
    uniswap_compatible_v4_sqrtpricex96_latest(
          blockchain = 'base'
        , project = 'pancakeswap'
        , version = 'infinity_cl'
        , PoolManager_evt_Initialize = source('pancakeswap_infinity_base', 'clpoolmanager_evt_initialize')
        , PoolManager_evt_Swap = source('pancakeswap_infinity_base', 'ClPoolManager_evt_Swap')
        , transactions = source('base', 'transactions')
    )
}}
