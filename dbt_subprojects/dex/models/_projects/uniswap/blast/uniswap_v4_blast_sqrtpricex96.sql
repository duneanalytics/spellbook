{{ config(
    schema = 'uniswap_v4_blast'
    , alias = 'sqrtpricex96'
    , materialized = 'table'
    , file_format = 'delta'
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}

{{
    uniswap_compatible_v4_liquidity_sqrtpricex96(
          blockchain = 'blast'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_Initialize = source('uniswap_v4_blast', 'PoolManager_evt_Initialize')
        , PoolManager_evt_Swap = source('uniswap_v4_blast', 'PoolManager_evt_Swap') 
        , transactions = source('blast', 'transactions')
        , monthly_relation = ref('uniswap_v4_blast_sqrtpricex96_monthly')
    )
}}