{{ config(
    schema = 'pancakeswap_infinity_cl_bnb'
    , alias = 'sqrtpricex96_monthly'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'id', 'block_month']
    )
}}

{{
    uniswap_compatible_v4_sqrtpricex96_monthly(
          blockchain = 'bnb'
        , project = 'pancakeswap'
        , version = 'infinity_cl'
        , PoolManager_evt_Initialize = source('pancakeswap_infinity_bnb', 'clpoolmanager_evt_initialize')
        , PoolManager_evt_Swap = source('pancakeswap_infinity_bnb', 'ClPoolManager_evt_Swap')
        , transactions = source('bnb', 'transactions')
    )
}}
