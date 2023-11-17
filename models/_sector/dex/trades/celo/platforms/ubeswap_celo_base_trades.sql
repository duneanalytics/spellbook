{{
    config(
        schema = 'ubeswap_celo',
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
        blockchain = 'celo',
        project = 'ubeswap',
        version = '1',
        Pair_evt_Swap = source('ubeswap_celo', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('ubeswap_celo', 'UbeswapFactory_evt_PairCreated')
    )
}}
