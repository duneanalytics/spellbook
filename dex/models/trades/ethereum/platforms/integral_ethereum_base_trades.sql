{{
    config(
        schema = 'integral_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'ethereum',
        project = 'integral',
        version = 'size',
        Pair_evt_Swap = source('integral_size_ethereum', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('integral_size_ethereum', 'Factory_evt_PairCreated')
    )
}}
