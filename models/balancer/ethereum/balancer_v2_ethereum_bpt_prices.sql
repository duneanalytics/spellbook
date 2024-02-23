{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_v2_ethereum',
        alias = 'bpt_prices',        
        materialized = 'table',
        file_format = 'delta'
    )
}}


{{ 
    balancer_bpt_prices_macro(
        blockchain = blockchain,
        version = '2'
    )
}}
