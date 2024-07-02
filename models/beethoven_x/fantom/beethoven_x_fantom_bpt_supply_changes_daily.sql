{{
    config(
        schema = 'beethoven_x_fantom',
        alias = 'bpt_supply_changes_daily', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

{{ 
    balancer_v2_compatible_bpt_supply_changes_daily_agg_macro(
        blockchain = 'fantom',
        version = '2',
        project_decoded_as = 'beethoven_x',
        base_spells_namespace = 'beethoven_x_fantom'
    )
}}