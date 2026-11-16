{{ config(
    schema = 'uniswap_v2_blast'
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
        , version = '2'
        , hardcoded_fee = 0.3
        , pool_column_name = 'pair'
        , token0_column_name = 'token0'
        , token1_column_name = 'token1'
        , pool_created_event = source('uniswap_v2_blast', 'UniswapV2Factory_evt_PairCreated')
    )
}}