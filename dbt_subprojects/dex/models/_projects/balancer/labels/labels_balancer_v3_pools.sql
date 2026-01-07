{{config(
        schema = 'labels',
        alias = 'balancer_v3_pools',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis"]\',
                                    "sector",
                                    "labels",
                                    \'["balancerlabs", "viniabussafi", "gosuto"]\') }}')}}

SELECT * FROM  {{ ref('labels_balancer_v3_pools_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v3_pools_gnosis') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v3_pools_arbitrum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v3_pools_base') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v3_pools_avalanche_c') }}
