{{
    config(
        schema = 'beethoven_x_fantom',
        alias = 'protocol_fee', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v2_compatible_protocol_fee_macro(
        blockchain = 'fantom',
        version = '2',        
        project_decoded_as = 'beethoven_x',
        base_spells_namespace = 'beethoven_x_fantom',
        pool_labels_spell = ref('labels_beethoven_x_pools_fantom')

    )
}}