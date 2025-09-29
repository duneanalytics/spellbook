{{ config(
    schema = 'pancakeswap_infinity_lb'
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
        , version = 'infinity_lb'
        , fee_column_name = 'fee'
        , pool_column_name = 'id'
        , token0_column_name = 'currency0'
        , token1_column_name = 'currency1'
        , pool_created_event = source('pancakeswap_infinity_bnb', 'binpoolmanager_evt_initialize')
        , hooks_column_name = 'hooks'
    )
}}