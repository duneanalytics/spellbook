{% set blockchain = 'base' %}

{{
    config(
        schema='balancer_v3_base',
        alias = 'protocol_fee', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v3_compatible_protocol_fee_macro(
        blockchain = blockchain,
        version = '3',        
        project_decoded_as = 'balancer_v3',
        base_spells_namespace = 'balancer',
        pool_labels_spell =  source('labels', 'balancer_v3_pools') 
    )
}}
