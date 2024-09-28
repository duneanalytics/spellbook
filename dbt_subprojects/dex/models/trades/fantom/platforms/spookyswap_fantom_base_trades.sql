{{
    config(
        schema = 'spookyswap_fantom',
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
        blockchain = 'fantom',
        project = 'spookyswap',
        version = '1',
        Pair_evt_Swap = source('spookyswap_fantom', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('spookyswap_fantom', 'UniswapV2Factory_evt_PairCreated')
    )
}}
