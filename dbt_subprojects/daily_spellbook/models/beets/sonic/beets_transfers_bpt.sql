{{
    config(
        schema = 'beets',
        alias = 'transfers_bpt',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

WITH v2 AS(
{{ 
    balancer_v2_compatible_transfers_bpt_macro(
        blockchain = 'sonic',
        version = '2',
        project_decoded_as = 'beethoven_x_v2'
    )
}}),

v3 AS (
{{ 
    balancer_v3_compatible_transfers_bpt_macro(
        blockchain = 'sonic',
        version = '3',
        project_decoded_as = 'beethoven_x_v3'
    )
}})

SELECT * FROM v2

UNION

SELECT * FROM v3