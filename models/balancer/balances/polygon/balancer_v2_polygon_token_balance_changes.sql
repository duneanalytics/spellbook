{% set blockchain = 'polygon' %}

{{ config(
        schema = 'balancer_v2_polygon',
        alias = 'bpt_token_balance_changes',
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_bpt_token_balance_changes_macro(
        blockchain = blockchain,
        version = '2'
    )
}}