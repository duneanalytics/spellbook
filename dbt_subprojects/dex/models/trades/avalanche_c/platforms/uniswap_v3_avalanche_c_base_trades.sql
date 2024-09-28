{{ config(
    schema = 'uniswap_v3_avalanche_c'
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
        blockchain = 'avalanche_c'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Swap = source('uniswap_v3_avalanche_c', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_avalanche_c', 'UniswapV3Factory_evt_PoolCreated')
    )
}}

-- refresh