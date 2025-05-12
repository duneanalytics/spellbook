{{ config(
    schema = 'velodrome_ink'
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
        blockchain = 'ink'
        , project = 'velodrome'
        , version = '3'
        , Pair_evt_Swap = source('velodrome_ink', 'clpool_evt_swap')
        , Factory_evt_PoolCreated = source('velodrome_ink', 'clfactory_evt_poolcreated')
    )
}}
