{{
    config(
        schema = 'sushiswap_nova',
        alias = 'base_trades',
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'nova',
        project = 'sushiswap',
        version = '1',
        Pair_evt_Swap = source('sushi_nova', 'uniswapv2pair_evt_swap'),
        Factory_evt_PairCreated = source('sushi_nova', 'uniswapv2factory_evt_paircreated')
    )
}}
