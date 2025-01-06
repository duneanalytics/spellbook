{{config(
        alias = 'balancer_gauges_zkevm',
        post_hook='{{ expose_spells(\'["zkevm"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski", "viniabussafi"]\') }}')}}

WITH gauges AS(
SELECT distinct
    'zkevm' AS blockchain,
    call.output_0 AS address,
    pools.address AS pool_address,
    child.output_0 AS child_gauge_address,    
    'zk:' || pools.name AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_zkevm' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'PolygonZkEVMRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_zkevm', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_zkevm') }} pools ON pools.address = child.pool)

    SELECT
          g.blockchain
         , g.address
         , g.pool_address
         , g.child_gauge_address
         , g.name
         , CASE WHEN k.call_success
            THEN 'inactive'
            ELSE 'active'
            END AS status
         , g.category
         , g.contributor
         , g.source
         , g.created_at
         , g.updated_at
         , g.model_name
         , g.label_type
    FROM gauges g
    LEFT JOIN {{ source('balancer_ethereum', 'PolygonZkEVMRootGauge_call_killGauge') }} ON g.address = k.contract_address