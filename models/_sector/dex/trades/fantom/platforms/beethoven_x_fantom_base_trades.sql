{{
    config(
        schema = 'beethoven_x_fantom',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    balancer_compatible_v2_trades(
        blockchain = 'fantom',
        project = 'beethoven_x',
        version = '1',
        project_decoded_as = 'beethoven_x',
        Vault_evt_Swap = 'Vault_evt_Swap',
        pools_fees = 'pools_fees'
    )
}}
