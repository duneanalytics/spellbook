{{
    config(
        schema = 'sushiswap_v2_blast',
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
        project = 'sushiswap',
        version = '2',
        Pair_evt_Swap = source('sushiswap_v2_blast', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('sushiswap_v2_blast', 'UniswapV2Factory_evt_PairCreated')
    )
}}
