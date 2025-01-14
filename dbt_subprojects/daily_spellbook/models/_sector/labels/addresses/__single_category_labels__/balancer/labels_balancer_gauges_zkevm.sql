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
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_zkevm') }} pools ON pools.address = child.pool),

kill_unkill_1 AS(
    SELECT
        contract_address,
        call_block_time,
        'kill' AS action
    FROM {{ source('balancer_ethereum', 'PolygonZkEVMRootGauge_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'PolygonZkEVMRootGauge_call_initialize') }}
    WHERE call_success

    UNION ALL

        SELECT
        target AS contract_address,
        evt_block_time AS call_block_time,
        'kill' AS action
    FROM {{ source('balancer_ethereum', 'AuthorizerAdaptorEntrypoint_evt_ActionPerformed') }}
    WHERE data = 0xab8f0945

    UNION ALL

        SELECT
        target AS contract_address,
        evt_block_time AS call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'AuthorizerAdaptorEntrypoint_evt_ActionPerformed') }}
    WHERE data = 0xd34fb267        
),

kill_unkill AS(
    SELECT
        contract_address,
        call_block_time,
        action,
        ROW_NUMBER() OVER(PARTITION BY contract_address ORDER BY call_block_time DESC) AS rn
    FROM kill_unkill_1
)

    SELECT DISTINCT
          g.blockchain
         , g.address
         , g.pool_address
         , g.child_gauge_address
         , g.name
         , CASE WHEN k.action = 'kill'
            THEN 'inactive'
           WHEN k.action = 'unkill'
            THEN 'active'
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
    LEFT JOIN kill_unkill k ON g.address = k.contract_address AND k.rn = 1