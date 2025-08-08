{{ config(
    schema = 'uniswap_v3_avalanche_c'
    , alias = 'pools'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

{{
    uniswap_compatible_pools(
          blockchain = 'avalanche_c'
        , project = 'uniswap'
        , version = '3'
        , fee_column_name = 'fee'
        , pool_column_name = 'pool'
        , token0_column_name = 'token0'
        , token1_column_name = 'token1'
        , pool_created_event = source('uniswap_v3_avalanche_c', 'UniswapV3Factory_evt_PoolCreated')
    )
}}