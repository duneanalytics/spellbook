{{config(
        schema = 'labels',
        alias = 'labels_balancer_v1_pools'
        , post_hook='{{ hide_spells() }}'
    )
}}

SELECT * FROM  {{ ref('labels_balancer_v1_pools_ethereum') }}