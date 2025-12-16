{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'lbps',
        materialized = 'table',
        file_format = 'delta'
    )
}}


{{ 
    balancer_v3_compatible_lbps_macro(
        blockchain = blockchain,
        project_decoded_as = 'balancer_v3'
    )
}}