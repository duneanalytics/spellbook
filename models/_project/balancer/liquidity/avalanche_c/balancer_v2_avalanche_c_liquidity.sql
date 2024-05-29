
{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'balancer_v2_avalanche_c',
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_liquidity_macro(
        blockchain = blockchain,
        version = '2'
    )
}}
