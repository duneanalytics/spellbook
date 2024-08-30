{{
    config(
        schema = 'swapr_ethereum',
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
        project = 'swapr',
        version = '1',
        Pair_evt_Swap = source('swapr_ethereum', 'DXswapPair_evt_Swap'),
        Factory_evt_PairCreated = source('swapr_ethereum', 'DXswapFactory_evt_PairCreated')
    )
}}
