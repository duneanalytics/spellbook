{{ config(
    schema = 'uniswap_v3_arbitrum'
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
        blockchain = 'arbitrum'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Swap = source('uniswap_v3_arbitrum', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_arbitrum', 'Factory_evt_PoolCreated')
    )
}}

