{% set blockchain = 'sei' %}

{{ config(
        schema = 'jelly_swap_' + blockchain,
        alias = 'bpt_supply',
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v2_compatible_bpt_supply_macro(
        blockchain = blockchain,
        version = '2',        
        project_decoded_as = 'jelly_swap',
        pool_labels_model = 'jelly_swap_pools_sei',
        transfers_spell = ref('jelly_swap_sei_transfers_bpt')
    )
}}