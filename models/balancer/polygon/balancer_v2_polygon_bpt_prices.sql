{% set blockchain = 'polygon' %}

{{
    config(
        schema = 'balancer_v2_polygon',
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
