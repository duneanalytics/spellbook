{{
    config(
        schema = 'beets_sonic',
        alias = 'bpt_supply_changes_daily', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH v2 AS(
{{ 
    balancer_v2_compatible_bpt_supply_changes_daily_agg_macro(
        blockchain = 'sonic',
        version = '2',
        project_decoded_as = 'beethoven_x_v2',
        base_spells_namespace = 'beets'
    )
}}),

v3 AS(
{{ 
    balancer_v3_compatible_bpt_supply_changes_daily_agg_macro(
        blockchain = 'sonic',
        version = '3',
        project_decoded_as = 'beethoven_x_v3',
        base_spells_namespace = 'beets'
    )
}})

SELECT * FROM v2

UNION

SELECT * FROM v3