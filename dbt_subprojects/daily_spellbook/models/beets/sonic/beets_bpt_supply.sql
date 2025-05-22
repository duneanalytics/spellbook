{{ config(
        schema = 'beets',
        alias = 'bpt_supply',
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH v2 AS(
{{ 
    balancer_v2_compatible_bpt_supply_macro(
        blockchain = 'sonic',
        version = '2',        
        project_decoded_as = 'beethoven_x_v2',
        pool_labels_model = 'beets_pools_sonic',
        transfers_spell = ref('beets_transfers_bpt')
    )
}}),

v3 AS({{ 
    balancer_v3_compatible_bpt_supply_macro(
        blockchain = 'sonic',
        version = '3',        
        project_decoded_as = 'beethoven_x_v3',
        pool_labels_model = 'beets_pools_sonic',
        transfers_spell = ref('beets_transfers_bpt')
    )
}})

SELECT * FROM v2

UNION

SELECT * FROM v3