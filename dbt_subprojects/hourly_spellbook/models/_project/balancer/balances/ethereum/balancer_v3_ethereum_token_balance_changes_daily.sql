{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_v3_ethereum',
        alias = 'token_balance_changes_daily', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v3_compatible_token_balance_changes_daily_agg_macro(
        blockchain = blockchain,
        version = '3',
        project_decoded_as = 'balancer_v3',
        base_spells_namespace = 'balancer'
    )
}}