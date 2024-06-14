{% set blockchain = 'ethereum' %}

{{
    config(
        schema = 'balancer_v2_ethereum',
        alias = 'bpt_supply_changes_daily', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    bpt_supply_changes_daily_agg_macro(
        blockchain = blockchain,
        version = '2'
    )
}}