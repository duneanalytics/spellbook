{{config(
        alias = 'balancer_gauges_gnosis',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "labels",
                                    \'["viniabussafi"]\') }}')}}

WITH gauges AS(
SELECT distinct
    'gnosis' AS blockchain,
    call.output_0 AS address,
    COALESCE(v2pools.address, v3pools.address) AS pool_address,
    child.output_0 AS child_gauge_address,    
    'gno:' || COALESCE(v2pools.name, v3pools.name) AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13' AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_gnosis' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'GnosisRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_gnosis', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_gnosis') }} v2pools ON v2pools.address = child.pool
    LEFT JOIN {{ source('labels', 'balancer_v3_pools_gnosis') }} v3pools ON v3pools.address = child.pool),

kill_unkill_1 AS(
    SELECT
        contract_address,
        call_block_time,
        'kill' AS action
    FROM {{ source('balancer_ethereum', 'GnosisRootGauge_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'GnosisRootGauge_call_initialize') }}
    WHERE call_success
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