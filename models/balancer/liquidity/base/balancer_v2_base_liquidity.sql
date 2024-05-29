
{% set blockchain = 'base' %}

{{
    config(
        schema = 'balancer_v2_base',
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
