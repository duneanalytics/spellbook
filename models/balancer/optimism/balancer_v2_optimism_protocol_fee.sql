{% set blockchain = 'optimism' %}

{{
    config(
        schema = 'balancer_v2_optimism',
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
