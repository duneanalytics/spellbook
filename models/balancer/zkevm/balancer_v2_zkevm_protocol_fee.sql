{% set blockchain = 'zkevm' %}

{{
    config(
        schema = 'balancer_v2_zkevm',
        alias = 'protocol_fee', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_protocol_fee_macro(
        blockchain = blockchain,
        version = '2'
    )
}}