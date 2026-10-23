{{
    config(
        schema = 'communityv3_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- CommunityV3 is an unmodified Uniswap V3 deployment (factory 0x00a38533c68327aBE0Cc265436e036066E2D6B38)
{{
    uniswap_compatible_v3_trades(
        blockchain = 'ethereum',
        project = 'communityv3',
        version = '3',
        Pair_evt_Swap = source('communityv3_ethereum', 'uniswapv3pool_evt_swap'),
        Factory_evt_PoolCreated = source('communityv3_ethereum', 'uniswapv3factory_evt_poolcreated')
    )
}}
