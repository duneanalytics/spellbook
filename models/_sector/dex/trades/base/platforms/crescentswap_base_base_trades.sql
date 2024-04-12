{{ config(
    schema = 'crescentswap_base'
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
        , project = 'crescent'
        , version = '1'
        , Pair_evt_Swap = source('crescent_base', 'pool_evt_Swap')
        , Factory_evt_PoolCreated = source('crescent_base', 'factory_evt_PoolCreated')
    )
}}