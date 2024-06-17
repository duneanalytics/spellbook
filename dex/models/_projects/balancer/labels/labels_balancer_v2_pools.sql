{{config(
        schema = 'labels',
        alias = 'balancer_v2_pools',        
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "optimism", "polygon", "avalanche_c", "base", "gnosis", "zkevm"]\',
                                    "sector",
                                    "labels",
                                    \'["balancerlabs", "viniabussafi"]\') }}')}}

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
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_pools_zkevm') }}