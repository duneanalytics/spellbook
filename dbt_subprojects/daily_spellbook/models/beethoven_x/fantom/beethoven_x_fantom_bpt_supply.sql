{{ config(
        schema = 'beethoven_x_fantom',
        alias = 'bpt_supply',
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v2_compatible_bpt_supply_macro(
        blockchain = 'fantom',
        version = '2',        
        project_decoded_as = 'beethoven_x',
        pool_labels_model = 'beethoven_x_pools_fantom',
        transfers_spell = ref('beethoven_x_fantom_transfers_bpt')
    )
}}