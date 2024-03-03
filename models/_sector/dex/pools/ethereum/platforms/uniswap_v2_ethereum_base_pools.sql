{{
    config(
        schema = 'uniswap_v2_ethereum',
        alias = 'base_pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_pools(
        blockchain = 'ethereum',
        project = 'uniswap',
        version = '2',
        Factory_evt_PairCreated = source('uniswap_v2_ethereum', 'Factory_evt_PairCreated')
    )
}}
