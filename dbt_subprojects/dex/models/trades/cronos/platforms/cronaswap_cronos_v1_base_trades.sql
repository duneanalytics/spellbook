{{ config(
    schema = 'cronaswap_v1_cronos'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'cronos'
        , project = 'cronaswap'
        , version = '1'
        , Pair_evt_Swap = source('cronaswap_cronos', 'CronaSwapPair_evt_Swap')
        , Factory_evt_PairCreated = source('cronaswap_cronos', 'CronaSwapFactoryV1_evt_PairCreated')
    )
}}
