{{ config(
    schema = 'derpdex_base'
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
          blockchain = 'base'
        , project = 'derpdex'
        , version = '3'
        , Pair_evt_Swap = source('derpdex_base', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('derpdex_base', 'UniswapV3Factory_evt_PoolCreated')
    )
}}