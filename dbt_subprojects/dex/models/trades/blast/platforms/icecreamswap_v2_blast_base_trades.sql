{{
    config(
        schema = 'icecreamswap_v2_blast',
        alias = 'base_trades',
        materialized = 'table',
        file_format = 'delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'blast',
        project = 'icecreamswap',
        version = '2',
        Pair_evt_Swap = source('icecreamswap_v2_blast', 'IceCreamSwapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('icecreamswap_v2_blast', 'IceCreamSwapV2Factory_evt_PairCreated')
    )
}}
