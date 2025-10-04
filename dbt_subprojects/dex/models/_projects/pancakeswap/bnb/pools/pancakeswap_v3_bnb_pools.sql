{{ config(
    schema = 'pancakeswap_v3_bnb'
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
          blockchain = 'bnb'
        , project = 'pancakeswap'
        , version = '3'
        , fee_column_name = 'fee'
        , pool_column_name = 'pool'
        , token0_column_name = 'token0'
        , token1_column_name = 'token1'
        , pool_created_event = source('pancakeswap_v3_bnb', 'PancakeV3Factory_evt_PoolCreated')
    )
}}