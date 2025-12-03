{{ config(
    schema = 'pancakeswap_v2_arbitrum'
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
        , version = '2'
        , hardcoded_fee = 0.25
        , pool_column_name = 'pair'
        , token0_column_name = 'token0'
        , token1_column_name = 'token1'
        , pool_created_event = source('pancakeswap_v2_arbitrum', 'PancakeFactory_evt_PairCreated')
    )
}}