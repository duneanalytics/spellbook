{{
    config(
        schema = 'icecreamswap_boba',
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
        blockchain = 'boba',
        project = 'icecreamswap',
        version = '1',
        Pair_evt_Swap = source('icecreamswap_boba', 'IceCreamSwapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('icecreamswap_boba', 'IceCreamSwapV2Factory_evt_PairCreated')
    )
}}
