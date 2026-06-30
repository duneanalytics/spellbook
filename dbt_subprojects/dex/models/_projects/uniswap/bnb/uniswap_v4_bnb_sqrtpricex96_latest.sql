{{ config(
    schema = 'uniswap_v4_bnb'
    , alias = 'sqrtpricex96_latest'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'id']
    )
}}

{{
    uniswap_compatible_v4_sqrtpricex96_latest(
          blockchain = 'bnb'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_Initialize = source('uniswap_v4_bnb', 'PoolManager_evt_Initialize')
        , PoolManager_evt_Swap = source('uniswap_v4_bnb', 'PoolManager_evt_Swap')
        , transactions = source('bnb', 'transactions')
    )
}}
