{{
    config(
        schema='beethoven_x_fantom',
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["fantom"]\',
                        "project",
                        "beethoven_x",
                        \'["viniabussafi"]\') }}'
    )
}}

{{ 
    balancer_v2_compatible_liquidity_macro(
        blockchain = 'fantom',
        version = '2',        
        project_decoded_as = 'beethoven_x',
        base_spells_namespace = 'beethoven_x_fantom',
        pool_labels_spell = 'labels_beethoven_x_pools_fantom'
    )
}}