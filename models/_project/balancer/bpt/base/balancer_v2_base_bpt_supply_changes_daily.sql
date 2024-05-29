{% set blockchain = 'base' %}

{{
    config(
        schema = 'balancer_v2_base',
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