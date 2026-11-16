{{
    config(
        schema = 'dackieswap_v2_blast',
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
        project = 'dackieswap',
        version = '2',
        Pair_evt_Swap = source('dackieswap_v2_blast', 'DackiePair_evt_Swap'),
        Factory_evt_PairCreated = source('dackieswap_v2_blast', 'DackieFactory_evt_PairCreated')
    )
}}
