{{
    config(
        schema = 'swapblast_blast',
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
        project = 'swapblast',
        version = '1',
        Pair_evt_Swap = source('swapblast_blast', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('swapblast_blast', 'UniswapV2Factory_evt_PairCreated')
    )
}}
