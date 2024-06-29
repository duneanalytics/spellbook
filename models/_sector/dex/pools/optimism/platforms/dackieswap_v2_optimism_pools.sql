{{ config(
    schema = 'dackieswap_v2_optimism',
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
        , version = '2'
        , Factory_evt_PairCreated = source('dackieswap_v2_optimism', 'DackieFactory_evt_PairCreated')
        , hardcoded_fee = 0.3
    )
}}