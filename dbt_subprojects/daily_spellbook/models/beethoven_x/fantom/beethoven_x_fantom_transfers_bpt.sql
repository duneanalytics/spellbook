{{
    config(
        schema = 'beethoven_x_fantom',
        alias = 'transfers_bpt',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

{{ 
    balancer_v2_compatible_transfers_bpt_macro(
        blockchain = 'fantom',
        version = '2',
        project_decoded_as = 'beethoven_x'
    )
}}