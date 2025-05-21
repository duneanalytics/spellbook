{% set blockchain = 'sonic' %}

{{
    config(
        schema = 'beets',
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH v2 AS(
{{ 
    balancer_v2_compatible_liquidity_macro(
        blockchain = blockchain,
        version = '2',        
        project_decoded_as = 'beethoven_x_v2',
        base_spells_namespace = 'beets',
        pool_labels_model = 'beets_pools_sonic'
    )
}}),

v3 AS(
{{ 
    balancer_v3_compatible_liquidity_macro(
        blockchain = blockchain,
        version = '3',        
        project_decoded_as = 'beethoven_x_v3',
        base_spells_namespace = 'beets',
        pool_labels_model = 'beets_pools_sonic'
    )
}})


SELECT * FROM v2

UNION

SELECT * FROM v3