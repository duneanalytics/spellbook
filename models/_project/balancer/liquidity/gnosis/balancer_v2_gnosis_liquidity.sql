
{% set blockchain = 'gnosis' %}

{{
    config(
    schema = 'balancer_v2_gnosis',
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
