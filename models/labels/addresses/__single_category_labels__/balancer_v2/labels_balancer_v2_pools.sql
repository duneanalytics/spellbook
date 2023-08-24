{{config(alias = alias('balancer_v2_pools'),
        tags = ['dunesql'],
        post_hook='{{ expose_spells(\'["ethereum","arbitrum","optimism", "polygon", "gnosis", "avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["balancerlabs"]\') }}')}}

SELECT * FROM  {{ ref('labels_balancer_v2_pools_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_pools_arbitrum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_pools_optimism') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_pools_polygon') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_pools_gnosis') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_pools_avalanche_c') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_pools_base') }}