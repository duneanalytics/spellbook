{{
    config(
        schema = 'dackieswap_v3_blast',
        alias = 'base_trades',
        materialized = 'table',
        file_format = 'delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'blast',
        project = 'dackieswap',
        version = '3',
        Pair_evt_Swap = source('dackieswap_v3_blast', 'DackieV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('dackieswap_v3_blast', 'DackieV3Factory_evt_PoolCreated')
    )
}}
