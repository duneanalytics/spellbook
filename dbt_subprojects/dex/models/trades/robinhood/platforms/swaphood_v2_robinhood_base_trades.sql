{{
    config(
        schema = 'swaphood_v2_robinhood',
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
        blockchain = 'robinhood',
        project = 'swaphood',
        version = '2',
        Pair_evt_Swap = source('swaphood_robinhood', 'SwapHoodPair_evt_Swap'),
        Factory_evt_PairCreated = source('swaphood_robinhood', 'SwapHoodFactory_evt_PairCreated')
    )
}}
