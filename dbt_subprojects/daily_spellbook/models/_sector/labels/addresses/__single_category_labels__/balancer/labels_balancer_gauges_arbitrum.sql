{{config(
        alias = 'balancer_gauges_arbitrum',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski", "viniabussafi"]\') }}')}}

WITH reward_gauges AS(
SELECT distinct
    'arbitrum' AS blockchain,
    gauge.gauge AS address,
    COALESCE(v2pools.address, v3pools.address) AS pool_address,
    streamer.gauge AS child_gauge_address,
    'arb:' || COALESCE(v2pools.name, v3pools.name) AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_arbitrum' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'ArbitrumRootGaugeFactory_evt_ArbitrumRootGaugeCreated') }} gauge
    LEFT JOIN {{ source('balancer_v2_arbitrum', 'ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated') }} streamer ON gauge.recipient = streamer.streamer
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_arbitrum') }} v2pools ON v2pools.address = streamer.pool
    LEFT JOIN {{ source('labels', 'balancer_v3_pools_arbitrum') }} v3pools ON v3pools.address = streamer.pool
WHERE COALESCE(v2pools.name, v3pools.name) IS NOT NULL

UNION ALL

SELECT distinct
    'arbitrum' AS blockchain,
    gauge.gauge AS address,
    COALESCE(v2pools.address, v3pools.address) AS pool_address,
    streamer.gauge AS child_gauge_address,
    'arb:' || COALESCE(v2pools.name, v3pools.name) AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_arbitrum' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'CappedArbitrumRootGaugeFactory_evt_GaugeCreated') }} gauge
    INNER JOIN {{ source('balancer_ethereum', 'CappedArbitrumRootGaugeFactory_call_create') }} call ON call.call_tx_hash = gauge.evt_tx_hash
    LEFT JOIN {{ source('balancer_v2_arbitrum', 'ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated') }} streamer ON streamer.streamer = call.recipient
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_arbitrum') }} v2pools ON v2pools.address = streamer.pool
    LEFT JOIN {{ source('labels', 'balancer_v3_pools_arbitrum') }} v3pools ON v3pools.address = streamer.pool
WHERE COALESCE(v2pools.name, v3pools.name) IS NOT NULL),

child_gauges AS(
SELECT distinct
    'arbitrum' AS blockchain,
    call.output_0 AS address,
    COALESCE(v2pools.address, v3pools.address) AS pool_address,
    child.output_0 AS child_gauge_address,
    'arb:' || COALESCE(v2pools.name, v3pools.name) AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_arbitrum' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'CappedArbitrumRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_arbitrum', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_arbitrum') }} v2pools ON v2pools.address = child.pool
    LEFT JOIN {{ source('labels', 'balancer_v3_pools_arbitrum') }} v3pools ON v3pools.address = child.pool
),

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
    FROM {{ source('balancer_ethereum', 'ArbitrumRootGauge_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'kill' AS action
    FROM {{ source('balancer_ethereum', 'CappedArbitrumRootGauge_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'ArbitrumRootGauge_call_initialize') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'CappedArbitrumRootGauge_call_initialize') }}
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