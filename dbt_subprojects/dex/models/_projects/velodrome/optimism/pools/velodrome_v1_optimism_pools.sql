{{ config(
    schema = 'velodrome_v1_optimism'
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
          blockchain = 'optimism'
        , project = 'velodrome'
        , version = '1'
        , pool_column_name = 'pair'
        , token0_column_name = 'token0'
        , token1_column_name = 'token1'
        , pool_created_event = source('velodrome_optimism', 'PairFactory_evt_PairCreated')
    )
}}