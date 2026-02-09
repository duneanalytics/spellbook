{% set blockchain = 'avalanche_c' %}

{{ config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'token_balance_changes',
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v2_compatible_token_balance_changes_macro(
        blockchain = blockchain,
        version = '3',
        project_decoded_as = 'balancer_v3',
        base_spells_namespace = 'balancer',
        pool_labels_model = 'balancer_v3_pools_avalanche_c'
    )
}}