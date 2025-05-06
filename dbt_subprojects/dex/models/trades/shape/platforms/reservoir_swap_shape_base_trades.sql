{{ config(
    schema = 'reservoir_swap_shape'
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
        blockchain = 'shape'
        , project = 'reservoir_swap'
        , version = '3'
        , Pair_evt_Swap = source('reservoir_swap_shape', 'uniswapv3pool_evt_swap')
        , Factory_evt_PoolCreated = source('reservoir_swap_shape', 'uniswapv3factory_evt_poolcreated')
    )
}} 