{{config(
        schema = 'labels',
        alias = 'balancer_v3_pools',        
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "optimism", "polygon", "avalanche_c", "base", "gnosis", "zkevm"]\',
                                    "sector",
                                    "labels",
                                    \'["balancerlabs", "viniabussafi"]\') }}')}}

SELECT * FROM  {{ ref('labels_balancer_v3_pools_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v3_pools_gnosis') }}