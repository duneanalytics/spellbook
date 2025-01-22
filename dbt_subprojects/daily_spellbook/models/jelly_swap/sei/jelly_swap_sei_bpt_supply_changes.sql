{% set blockchain = 'sei' %}

{{
    config(
        schema = 'jelly_swap_' + blockchain,
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
        blockchain = blockchain,
        version = '2',
        project_decoded_as = 'jelly_swap',
        base_spells_namespace = 'jelly_swap_sei',
        pool_labels_spell = ref('labels_jelly_swap_pools_sei')
    )
}}