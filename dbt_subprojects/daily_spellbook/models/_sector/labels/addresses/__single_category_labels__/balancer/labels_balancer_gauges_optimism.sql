{{config(
        alias = 'balancer_gauges_optimism',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski", "viniabusafi"]\') }}')}}

WITH reward_gauges AS(
SELECT
    'optimism' AS blockchain,
    gauge.gauge AS address,
    pools.address AS pool_address,
    streamer.gauge AS child_gauge_address,
    'op:' || pools.name AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_optimism' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'OptimismRootGaugeFactory_evt_OptimismRootGaugeCreated') }} gauge
    LEFT JOIN {{ source('balancer_optimism', 'ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated') }} streamer ON gauge.recipient = streamer.streamer
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_optimism') }} pools ON pools.address = streamer.pool

UNION ALL

SELECT
    'optimism' AS blockchain,
    gauge.gauge AS address,
    pools.address AS pool_address,
    streamer.gauge AS child_gauge_address,
    'op:' || pools.name AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_optimism' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'CappedOptimismRootGaugeFactory_evt_GaugeCreated') }} gauge
    INNER JOIN {{ source('balancer_ethereum', 'CappedOptimismRootGaugeFactory_call_create') }} call ON call.call_tx_hash = gauge.evt_tx_hash
    LEFT JOIN {{ source('balancer_optimism', 'ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated') }} streamer ON streamer.streamer = call.recipient
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_optimism') }} pools ON pools.address = streamer.pool),

child_gauges AS(
SELECT distinct
    'optimism' AS blockchain,
    call.output_0 AS address,
    pools.address AS pool_address,
    child.output_0 AS child_gauge_address,
    'opt:' || pools.name AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_optimism' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'CappedOptimismRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_optimism', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_optimism') }} pools ON pools.address = child.pool),

gauges AS(
    SELECT 
        * 
    FROM reward_gauges
    WHERE name IS NOT NULL
    
    UNION ALL

    SELECT 
        * 
    FROM child_gauges
    WHERE name IS NOT NULL),

kill_unkill_1 AS(
    SELECT
        contract_address,
        call_block_time,
        'kill' AS action
    FROM {{ source('balancer_ethereum', 'OptimismRootGauge_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'kill' AS action
    FROM {{ source('balancer_ethereum', 'CappedOptimismRootGauge_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'OptimismRootGauge_call_initialize') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'CappedOptimismRootGauge_call_initialize') }}
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