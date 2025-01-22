{{
    config(
        schema = 'beethoven_x_fantom',
        alias = 'bpt_supply_changes', 
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'evt_tx_hash', 'evt_index', 'label'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')]
    )
}}

{{ 
    balancer_v2_compatible_bpt_supply_changes_macro(
        blockchain = 'fantom',
        version = '2',
        project_decoded_as = 'beethoven_x',
        base_spells_namespace = 'beethoven_x_fantom',
        pool_labels_spell = ref('labels_beethoven_x_pools_fantom')
    )
}}