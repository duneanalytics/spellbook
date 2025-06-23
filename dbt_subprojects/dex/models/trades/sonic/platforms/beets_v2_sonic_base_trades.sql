{{
    config(
        schema = 'beets_v2_sonic',
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
        blockchain = 'sonic',
        project = 'beets',
        project_decoded_as = 'beethoven_x_v2',
        version = '2'
    )
}}
