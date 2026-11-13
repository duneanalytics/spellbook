{{
    config(
        schema = 'piperx_v2_story',
        alias = 'base_trades',
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'story',
        project = 'piperx',
        version = '2',
        Pair_evt_Swap = source('piperx_story', 'uniswapv2pool_evt_swap'),
        Factory_evt_PairCreated = source('piperx_story', 'piperxswapv2factory_evt_paircreated')
    )
}}
