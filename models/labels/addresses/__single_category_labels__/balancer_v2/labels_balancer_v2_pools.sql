{{config(
	tags=['legacy'],
	alias = alias('balancer_v2_pools', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum","arbitrum","optimism", "polygon"]\',
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