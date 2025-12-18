{{ config(
    schema = 'uniswap_v4_monad'
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
          blockchain = 'monad'
        , project = 'uniswap'
        , version = '4'
        , fee_column_name = 'fee'
        , pool_column_name = 'id'
        , token0_column_name = 'currency0'
        , token1_column_name = 'currency1'
        , pool_created_event = source('uniswap_v4_monad', 'PoolManager_evt_Initialize')
        , hooks_column_name = 'hooks'
    )
}}