{% set blockchain = 'sei' %}

{{
    config(
        schema = 'jelly_swap_' + blockchain,
        alias = 'bpt_supply_changes_daily', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v2_compatible_bpt_supply_changes_daily_agg_macro(
        blockchain = blockchain,
        version = '2',
        project_decoded_as = 'jelly_swap',
        base_spells_namespace = 'jelly_swap_sei'
    )
}}