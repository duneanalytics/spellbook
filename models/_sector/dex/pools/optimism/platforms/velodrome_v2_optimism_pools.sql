{{ config(
    schema = 'velodrome_v2_optimism',
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
        , project = 'velodrome'
        , version = '2'
        , Factory_evt_PairCreated = source('velodrome_v2_optimism', 'PoolFactory_evt_PoolCreated')
        , pool_column_name = 'pool'
        , hardcoded_fee = 0.02
    )
}}