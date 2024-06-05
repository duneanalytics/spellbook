
{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_v2_ethereum',
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