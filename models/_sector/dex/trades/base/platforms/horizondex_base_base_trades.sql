{{
    config(
    schema = 'horizondex_base'
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
        , project = 'horizondex'
        , version = '1'
        , Pair_evt_Swap = source('horizondex_base', 'Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('horizondex_base', 'Factory_evt_PoolCreated')
    )
}}