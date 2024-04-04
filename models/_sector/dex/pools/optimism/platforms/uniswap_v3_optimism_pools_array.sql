{{ config(
    schema = 'uniswap_v3_optimism',
    alias = 'pools_array',
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
        , project = 'uniswap'
        , version = '3'
        , Factory_evt_PairCreated = ref('uniswap_optimism_pools')
        , pool_column_name = 'pool'
        , fee_column_name = 'fee'
    )
}}