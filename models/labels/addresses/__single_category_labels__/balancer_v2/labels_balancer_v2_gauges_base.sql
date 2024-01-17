{{config(
        
        alias = 'balancer_v2_gauges_base',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "labels",
                                    \'["viniabussafi"]\') }}')}}

SELECT distinct
    'base' AS blockchain,
    call.output_0 AS address,
    'base:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_base' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'BaseRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_base', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ ref('labels_balancer_v2_pools_base') }} pools ON pools.address = child.pool
