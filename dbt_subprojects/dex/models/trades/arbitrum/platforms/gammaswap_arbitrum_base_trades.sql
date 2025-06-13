{{
    config(
        schema = 'gammaswap_arbitrum',
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
        blockchain = 'arbitrum',
        project = 'gammaswap',
        version = '2',
        Pair_evt_Swap = source('gammaswap_multichain', 'deltaswappair_evt_swap'),
        Factory_evt_PairCreated = source('gammaswap_multichain', 'deltaswapfactory_evt_paircreated')
    )
}}
