{{ config(
    schema = 'uniswap_v2_monad'
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
        , version = '2'
        , hardcoded_fee = 0.3
        , pool_column_name = 'pair'
        , token0_column_name = 'token0'
        , token1_column_name = 'token1'
        , pool_created_event = source('uniswap_v2_monad', 'uniswapv2factory_evt_paircreated')
    )
}}