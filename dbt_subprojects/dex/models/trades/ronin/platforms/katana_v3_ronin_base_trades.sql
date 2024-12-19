{{ 
    config(
    schema = 'katana_v3_ronin'
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
        blockchain = 'ronin'
        , project = 'katana'
        , version = '3'
        , Pair_evt_Swap = source('katana_dex_ronin', 'KatanaV3Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('katana_dex_ronin', 'KatanaV3Factory_evt_PoolCreated')
    )
}}