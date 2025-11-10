{{ config(
    schema = 'pancakeswap_stableswap_ethereum'
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
          blockchain = 'ethereum'
        , project = 'pancakeswap'
        , version = 'stableswap'
        , pool_column_name = 'swapContract'
        , token0_column_name = 'tokenA'
        , token1_column_name = 'tokenB'
        , pool_created_event = source('pancakeswap_v2_ethereum', 'PancakeStableSwapFactory_evt_NewStableSwapPair')
    )
}}

