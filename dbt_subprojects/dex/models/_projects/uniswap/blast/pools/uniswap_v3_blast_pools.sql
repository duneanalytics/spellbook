{{ config(
    schema = 'uniswap_v3_blast'
    , alias = 'pools'
    , materialized = 'table'
    , file_format = 'delta'
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}

{{
    uniswap_compatible_pools(
          blockchain = 'blast'
        , project = 'uniswap'
        , version = '3'
        , fee_column_name = 'fee'
        , pool_column_name = 'pool'
        , token0_column_name = 'token0'
        , token1_column_name = 'token1'
        , pool_created_event = source('uniswap_v3_blast', 'UniswapV3Factory_evt_PoolCreated')
    )
}}