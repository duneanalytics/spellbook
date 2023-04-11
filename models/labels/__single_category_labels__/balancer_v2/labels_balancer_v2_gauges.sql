{{config(alias='balancer_v2_gauges',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski"]\') }}')}}

SELECT * FROM  {{ ref('labels_balancer_v2_gauges_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_gauges_polygon') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_gauges_arbitrum') }}
UNION
SELECT * FROM  {{ ref('labels_balancer_v2_gauges_optimism') }}