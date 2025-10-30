{{ config(
    schema = 'abstractswap_v3_abstract'
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
        blockchain = 'abstract'
        , project = 'abstractswap'
        , version = '3'
        , Pair_evt_Swap = source('abstractswap_abstract', 'uniswapv3pool_evt_swap')
        , Factory_evt_PoolCreated = source('reservoir_swap_abstract', 'uniswapv3factory_evt_poolcreated')
    )
}}  