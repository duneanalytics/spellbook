
{% set blockchain = 'base' %}

{{
    config(
        schema = 'balancer_v3_base',
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