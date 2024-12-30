{{
    config(
        schema = 'beets_sonic',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


WITH v2_trades AS(
    {{
    balancer_compatible_v2_trades(
        blockchain = 'sonic',
        project = 'beets',
        version = '2',
        project_decoded_as = 'beethoven_x_v2',
        Vault_evt_Swap = 'Vault_evt_Swap',
        pools_fees = 'pools_fees'
    )
}}),

v3_trades AS(
    {{
    balancer_compatible_v3_trades(
        blockchain = 'sonic',
        project = 'beets',
        version = '3',
        project_decoded_as = 'beethoven_x_v3',
        Vault_evt_Swap = 'Vault_evt_Swap'
    )
}})

SELECT * FROM v2_trades

UNION

SELECT * FROM v3_trades