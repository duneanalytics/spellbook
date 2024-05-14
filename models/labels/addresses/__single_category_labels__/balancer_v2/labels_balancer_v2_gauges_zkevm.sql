{{config(
        alias = 'balancer_v2_gauges_zkevm',
        post_hook='{{ expose_spells(\'["zkevm"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski", "viniabussafi"]\') }}')}}

SELECT distinct
    'zkevm' AS blockchain,
    call.output_0 AS address,
    'zk:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_zkevm' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'PolygonZkEVMRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_zkevm', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ ref('labels_balancer_v2_pools_zkevm') }} pools ON pools.address = child.pool
