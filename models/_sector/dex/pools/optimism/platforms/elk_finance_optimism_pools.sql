{{ config(
    schema = 'elk_finance_optimism',
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
        , project = 'elk_finance'
        , version = '1'
        , Factory_evt_PairCreated = source('elk_finance_optimism', 'ElkFactory_evt_PairCreated')
        , hardcoded_fee = 0.3
    )
}}