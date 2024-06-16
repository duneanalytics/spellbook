{{ config(
    schema = 'gridex_optimism',
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
        , project = 'gridex'
        , version = '1'
        , Factory_evt_PairCreated = source('gridex_optimism', 'GridFactory_evt_GridCreated')
        , pool_column_name = 'grid'
        , hardcoded_fee = 0.3
    )
}}