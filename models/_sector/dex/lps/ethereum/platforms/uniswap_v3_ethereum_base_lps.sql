{{ config(
    schema = 'uniswap_v3_ethereum'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'ethereum'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Mint = source('uniswap_v3_ethereum', 'Pair_evt_Mint')
        , Pair_evt_Burn = source('uniswap_v3_ethereum', 'Pair_evt_Burn')
        , Factory_evt_PoolCreated = source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated')
        , NonfungibleTokenPositionManager_evt_Transfer = source('uniswap_v3_ethereum', 'NonfungibleTokenPositionManager_evt_Transfer')
        , NonfungibleTokenPositionManager_evt_IncreaseLiquidity = source('uniswap_v3_ethereum', 'NonfungibleTokenPositionManager_evt_IncreaseLiquidity')
        , NonfungibleTokenPositionManager_evt_DecreaseLiquidity = source('uniswap_v3_ethereum', 'NonfungibleTokenPositionManager_evt_DecreaseLiquidity')
        , position_manager_addr = 0xc36442b4a4522e871399cd717abdd847ab11fe88
    )
}}