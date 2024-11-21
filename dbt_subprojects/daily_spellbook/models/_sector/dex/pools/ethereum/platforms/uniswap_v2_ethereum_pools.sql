{{ config(
    schema = 'uniswap_v2_ethereum',
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
        blockchain = 'ethereum'
        , project = 'uniswap'
        , version = '2'
        , Factory_evt_PairCreated = source('uniswap_v2_ethereum', 'Factory_evt_PairCreated')
        , hardcoded_fee = 0.3
    )
}}