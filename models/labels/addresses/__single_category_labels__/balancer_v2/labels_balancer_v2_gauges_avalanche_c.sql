{{config(
        
        alias = 'balancer_v2_gauges_avalanche_c',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["viniabussafi"]\') }}')}}

SELECT distinct
    'avalanche_c' AS blockchain,
    call.output_0 AS address,
    'ava:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_avalanche_c' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'AvalancheRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_avalanche_c', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ ref('labels_balancer_v2_pools_avalanche_c') }} pools ON pools.address = child.pool
