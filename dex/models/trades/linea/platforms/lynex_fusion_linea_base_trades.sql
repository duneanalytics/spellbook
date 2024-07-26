{{
    config(
    schema = 'lynex_fusion_linea'
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
        blockchain = 'linea'
        , project = 'lynex'
        , version = 'fusion'
        , Pair_evt_Swap = source('lynex_linea', 'AlgebraPool_evt_Swap')
        , Factory_evt_PoolCreated = source('lynex_linea', 'AlgebraFactory_evt_Pool')
        , optional_columns = null
    )
}}