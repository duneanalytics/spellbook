{{
    config(
        schema = 'defi_kingdoms_kaia',
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
        blockchain = 'kaia',
        project = 'defi_kingdoms',
        version = '2',
        Pair_evt_Swap = source('defi_kingdoms_kaia', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('defi_kingdoms_kaia', 'UniswapV2Factory_evt_PairCreated')
    )
}}
