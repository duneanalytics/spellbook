{{ config(
    schema = 'uniswap_v4_unichain'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v4_trades(
        blockchain = 'unichain'
        , project = 'uniswap'
        , version = '4'
        , Pair_evt_Swap = source('uniswap_v4_unichain', 'UniswapV4Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v4_unichain', 'UniswapV4Factory_evt_PoolCreated')
    )
}}