{{
    config(
        schema = 'rcpswap_nova',
        alias = 'base_trades',
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'nova',
        project = 'rcpswap',
        version = '2',
        Pair_evt_Swap = source('rcpswap_nova', 'swappair_evt_swap'),
        Factory_evt_PairCreated = source('rcpswap_nova', 'swapfactory_evt_paircreated')
    )
}}
