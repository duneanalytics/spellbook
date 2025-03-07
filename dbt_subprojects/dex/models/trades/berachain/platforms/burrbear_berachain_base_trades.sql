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
    balancer_compatible_v2_trades(
        blockchain = 'berachain'
        , project = 'burrbear'
        , version = '1'
        , project_decoded_as = 'burrbear'
        , Vault_evt_Swap = 'vault_evt_swap'
        , pools_fees = 'pools_fees'
    )
}}
