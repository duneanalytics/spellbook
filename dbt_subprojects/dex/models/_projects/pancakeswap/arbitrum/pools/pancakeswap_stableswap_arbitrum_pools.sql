{{ config(
    schema = 'pancakeswap_stableswap_arbitrum'
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
          blockchain = 'arbitrum'
        , project = 'pancakeswap'
        , version = 'stableswap'
        , pool_column_name = 'swapContract'
        , token0_column_name = 'tokenA'
        , token1_column_name = 'tokenB'
        , pool_created_event = source('pancakeswap_v2_arbitrum', 'PancakeStableSwapFactory_evt_NewStableSwapPair')
    )
}}