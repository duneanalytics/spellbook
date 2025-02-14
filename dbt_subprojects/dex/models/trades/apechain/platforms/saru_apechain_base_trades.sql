{{ config(
    schema = 'saru_apechain'
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
        blockchain = 'apechain'
        , project = 'saru'
        , version = '1'
        , Pair_evt_Swap = source('saru_apechain', 'sarupair_evt_swap')
        , Factory_evt_PairCreated = source('saru_apechain', 'sarufactory_evt_paircreated')
    )
}}
