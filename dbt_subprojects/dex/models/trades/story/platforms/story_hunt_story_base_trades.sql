{{
    config(
        schema = 'story_hunt_story',
        alias = 'base_trades',
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'story',
        project = 'story_hunt',
        version = '1',
        Pair_evt_Swap = source('story_hunt_story', 'storyhuntv3pool_evt_swap'),
        Factory_evt_PoolCreated = source('story_hunt_story', 'storyhuntv3factory_evt_poolcreated')
    )
}}
