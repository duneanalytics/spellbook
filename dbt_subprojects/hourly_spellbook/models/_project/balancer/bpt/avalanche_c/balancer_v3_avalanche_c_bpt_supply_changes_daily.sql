{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'bpt_supply_changes_daily', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v2_compatible_bpt_supply_changes_daily_agg_macro(
        blockchain = blockchain,
        version = '3',
        project_decoded_as = 'balancer_v3',
        base_spells_namespace = 'balancer'
    )
}}