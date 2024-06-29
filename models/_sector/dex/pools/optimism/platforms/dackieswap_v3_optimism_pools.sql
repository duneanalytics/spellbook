{{ config(
    schema = 'dackieswap_v3_optimism',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

{{
    uniswap_compatible_pools(
        blockchain = 'optimism'
        , project = 'dackieswap'
        , version = '3'
        , Factory_evt_PairCreated = source('dackieswap_v3_optimism', 'DonaswapV3Factory_evt_PoolCreated')
        , pool_column_name = 'pool'
        , fee_column_name = 'fee'
    )
}}