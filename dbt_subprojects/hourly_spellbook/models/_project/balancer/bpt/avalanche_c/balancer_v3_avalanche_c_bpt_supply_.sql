{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema='avalanche_c',
        alias = 'bpt_supply',
        materialized = 'table',
        file_format = 'delta'

    )
}}

{{ 
    balancer_v3_compatible_bpt_supply_macro(
        blockchain = blockchain,
        version = '3',        
        project_decoded_as = 'balancer_v3',
        pool_labels_model = 'avalanche_c',
        transfers_spell = ref('avalanche_c_transfers_bpt')
    )
}}