{{
    config(
        schema = 'fenix_blast',
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
        project = 'fenix',
        version = '2',
        Pair_evt_Swap = source('fenix_blast', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('fenix_blast', 'PairFactoryUpgradeable_evt_PairCreated')
    )
}}
