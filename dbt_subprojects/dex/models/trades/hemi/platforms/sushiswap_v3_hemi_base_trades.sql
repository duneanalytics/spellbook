{{ config(
    schema = 'sushiswap_v3_hemi'
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
        blockchain = 'hemi'
        , project = 'sushiswap'
        , version = '3'
        , Pair_evt_Swap = source('sushiswap_hemi', 'v3pool_evt_swap')
        , Factory_evt_PoolCreated = source('sushiswap_hemi', 'v3factory_evt_poolcreated')
    )
}}