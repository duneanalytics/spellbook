{{config(
        schema = 'labels',
        alias = 'balancer_v3_pools'
        , post_hook='{{ hide_spells() }}'
    )
}}

SELECT * FROM  {{ ref('labels_balancer_v3_pools_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v3_pools_gnosis') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v3_pools_arbitrum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v3_pools_base') }}
UNION
SELECT * FROM {{ ref('labels_balancer_v3_pools_hyperevm') }}