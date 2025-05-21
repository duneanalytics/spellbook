{% set blockchain = 'gnosis' %}

{{
    config(
        schema='balancer_v3_gnosis',
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
        pool_labels_model = 'balancer_v3_pools_gnosis',
        transfers_spell = ref('balancer_v3_gnosis_transfers_bpt')
    )
}}