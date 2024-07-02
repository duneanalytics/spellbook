{% set blockchain = 'polygon' %}

{{
    config(
        schema = 'balancer_v2_polygon',
        alias = 'token_balance_changes_daily', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v2_compatible_token_balance_changes_daily_agg_macro(
        blockchain = blockchain,
        version = '2',
        project_decoded_as = 'balancer_v2',
        base_spells_namespace = 'balancer'
    )
}}