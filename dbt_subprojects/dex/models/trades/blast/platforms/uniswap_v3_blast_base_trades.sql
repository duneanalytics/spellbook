{{ config(
    schema = 'uniswap_v3_blast'
    , alias = 'base_trades'
    , materialized = 'table'
    , file_format = 'delta'
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'blast'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Swap = source('uniswap_v3_blast', 'UniswapV3Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_blast', 'UniswapV3Factory_evt_PoolCreated')
    )
}}
