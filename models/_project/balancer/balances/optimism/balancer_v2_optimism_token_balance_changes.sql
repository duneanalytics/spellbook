{% set blockchain = 'optimism' %}

{{ config(
        schema = 'balancer_v2_optimism',
        alias = 'token_balance_changes',
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_token_balance_changes_macro(
        blockchain = blockchain,
        version = '2'
    )
}}