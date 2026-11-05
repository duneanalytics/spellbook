{{ config(
    schema = 'lista_v3_bnb'
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
        blockchain = 'bnb'
        , project = 'lista'
        , version = '3'
        , Pair_evt_Swap = source('lista_bnb', 'listav3pool_evt_swap')
        , Factory_evt_PoolCreated = source('lista_bnb', 'listav3factory_evt_poolcreated')
    )
}}
