{{
    config(
        schema = 'beets_sonic',
        alias = 'bpt_supply_changes', 
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index', 'label'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

WITH v2 AS(
{{ 
    balancer_v2_compatible_bpt_supply_changes_macro(
        blockchain = 'sonic',
        version = '2',
        project_decoded_as = 'beethoven_x_v2',
        base_spells_namespace = 'beets',
        pool_labels_model = 'beets_pools_sonic'
    )
}}),

v3 AS(
{{ 
    balancer_v3_compatible_bpt_supply_changes_macro(
        blockchain = 'sonic',
        version = '3',
        project_decoded_as = 'beethoven_x_v3',
        base_spells_namespace = 'beets',
        pool_labels_model = 'beets_pools_sonic'
    )
}})

SELECT * FROM v2

UNION

SELECT * FROM v3