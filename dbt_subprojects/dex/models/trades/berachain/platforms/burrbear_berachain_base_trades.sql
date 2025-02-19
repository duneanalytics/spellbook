{{ config(
    schema = 'burrbear_berachain'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    balancer_compatible_v1_trades(
        blockchain = 'berachain'
        , project = 'burrbear'
        , version = '1'
        , Vault_evt_Swap = source('burrbear_berachain', 'vault_evt_swap')
        , Factory_evt_PoolCreated = source('burrbear_berachain', 'factory_evt_poolcreated')
    )
}}
