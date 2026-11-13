{{
    config(
        schema = 'dyorswap_blast',
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
        project = 'dyorswap',
        version = '1',
        Pair_evt_Swap = source('dyorswap_blast', 'DYORPair_evt_Swap'),
        Factory_evt_PairCreated = source('dyorswap_blast', 'DYORFactory_evt_PairCreated')
    )
}}
