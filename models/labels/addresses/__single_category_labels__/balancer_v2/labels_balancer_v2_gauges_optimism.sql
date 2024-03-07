{{config(
        
        alias = 'balancer_v2_gauges_optimism',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski", "viniabusafi"]\') }}')}}

WITH reward_gauges AS(
SELECT
    'optimism' AS blockchain,
    gauge.gauge AS address,
    'op:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_optimism' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'OptimismRootGaugeFactory_evt_OptimismRootGaugeCreated') }} gauge
    LEFT JOIN {{ source('balancer_optimism', 'ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated') }} streamer ON gauge.recipient = streamer.streamer
    LEFT JOIN {{ ref('labels_balancer_v2_pools_optimism') }} pools ON pools.address = streamer.pool

UNION ALL

SELECT
    'optimism' AS blockchain,
    gauge.gauge AS address,
    'op:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_optimism' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'CappedOptimismRootGaugeFactory_evt_GaugeCreated') }} gauge
    INNER JOIN {{ source('balancer_ethereum', 'CappedOptimismRootGaugeFactory_call_create') }} call ON call.call_tx_hash = gauge.evt_tx_hash
    LEFT JOIN {{ source('balancer_optimism', 'ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated') }} streamer ON streamer.streamer = call.recipient
    LEFT JOIN {{ ref('labels_balancer_v2_pools_optimism') }} pools ON pools.address = streamer.pool),

child_gauges AS(
SELECT distinct
    'optimism' AS blockchain,
    call.output_0 AS address,
    'opt:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_optimism' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'CappedOptimismRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_optimism', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ ref('labels_balancer_v2_pools_optimism') }} pools ON pools.address = child.pool)

SELECT * FROM reward_gauges
WHERE name IS NOT NULL
UNION ALL
SELECT * FROM child_gauges
WHERE name IS NOT NULL
