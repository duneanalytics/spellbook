{{ config(
    schema = 'uniswap_v4_ink'
    , alias = 'sqrtpricex96_monthly'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'id', 'block_month']
    )
}}

{{
    uniswap_compatible_v4_sqrtpricex96_monthly(
          blockchain = 'ink'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_Initialize = source('uniswap_v4_ink', 'PoolManager_evt_Initialize')
        , PoolManager_evt_Swap = source('uniswap_v4_ink', 'PoolManager_evt_Swap')
        , transactions = source('ink', 'transactions')
    )
}}
