{{ config(
    schema = 'velocimeter_v2_base'
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
        blockchain = 'base'
        , project = 'velocimeter'
        , version = '2'
        , Pair_evt_Swap = source('basevelocimeter_base', 'Pair_evt_Swap')
        , Factory_evt_PairCreated = source('basevelocimeter_base', 'PairFactory_evt_PairCreated')
    )
}}