{{ config(
    schema = 'solidly_v3_optimism',
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
        , project = 'solidly'
        , version = '3'
        , Factory_evt_PairCreated = source('solidly_v3_optimism', 'SolidlyV3Factory_evt_PoolCreated')
        , pool_column_name = 'pool'
        , fee_column_name = 'fee'
    )
}}