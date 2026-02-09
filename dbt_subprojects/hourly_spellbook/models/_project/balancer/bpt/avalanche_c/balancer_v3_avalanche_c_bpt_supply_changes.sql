{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'balancer_v3_avalanche_c',
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
        version = '3',
        project_decoded_as = 'balancer_v3',
        base_spells_namespace = 'balancer',
        pool_labels_model = 'balancer_v3_pools_avalanche_c'
    )
}}