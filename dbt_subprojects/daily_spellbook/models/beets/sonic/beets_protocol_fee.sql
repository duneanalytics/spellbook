{{
    config(
        schema = 'beets',
        alias = 'protocol_fee', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH v2 AS(
{{ 
    balancer_v2_compatible_protocol_fee_macro(
        blockchain = 'sonic',
        version = '2',        
        project_decoded_as = 'beethoven_x_v2',
        base_spells_namespace = 'beets',
        pool_labels_spell = ref('labels_beets_pools_sonic')

    )
}}),

v3 AS(
{{ 
    balancer_v3_compatible_protocol_fee_macro(
        blockchain = 'sonic',
        version = '3',        
        project_decoded_as = 'beethoven_x_v3',
        base_spells_namespace = 'beets',
        pool_labels_spell = ref('labels_beets_pools_sonic')

    )
}})

SELECT * FROM v2

UNION

SELECT * FROM v3
