
{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_v3_ethereum',
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta'
    )
}}


{{ 
    balancer_v3_compatible_liquidity_macro(
        blockchain = blockchain,
        version = '3',        
        project_decoded_as = 'balancer_v3',
        base_spells_namespace = 'balancer',
        pool_labels_spell =  source('labels', 'balancer_v3_pools') 
    )
}}