{{ config(
    schema = 'jetswap_polygon'
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
        blockchain = 'polygon',
        project = 'jetswap',
        version = '1',
        Pair_evt_Swap = source('jetswap_polygon', 'jetswap_polygon.JetswapPair_evt_Swap'),
        Factory_evt_PairCreated = source('jetswap_polygon', 'JetswapFactory_evt_PairCreated')
    )
}}
