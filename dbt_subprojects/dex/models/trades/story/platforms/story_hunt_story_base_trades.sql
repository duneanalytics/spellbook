{{
    config(
        schema = 'story_hunt_story',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
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
