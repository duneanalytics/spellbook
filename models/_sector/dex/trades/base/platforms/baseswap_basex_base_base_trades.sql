{{ config(
    schema = 'baseswap_basex_base'
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
        , project = 'baseswap'
        , version = 'basex'
        , Pair_evt_Swap = source('basex_base', 'UniswapV3Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('basex_base', 'UniswapV3Factory_evt_PoolCreated')
    )
}}