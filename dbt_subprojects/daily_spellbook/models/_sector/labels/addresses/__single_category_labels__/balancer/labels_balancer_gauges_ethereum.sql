{{config(
        alias = 'balancer_gauges_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski", "viniabussafi"]\') }}')}}

WITH gauges AS(
SELECT
    'ethereum' AS blockchain,
    gauge AS address,
    COALESCE(v2pools.address, v3pools.address) AS pool_address,
    CAST(NULL AS VARBINARY) AS child_gauge_address,
    'eth:' || COALESCE(v2pools.name, v3pools.name) AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_ethereum' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'LiquidityGaugeFactory_evt_GaugeCreated') }} gauge
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_ethereum') }} v2pools ON v2pools.address = gauge.pool
    LEFT JOIN {{ source('labels', 'balancer_v3_pools_ethereum') }} v3pools ON v3pools.address = gauge.pool

UNION ALL

SELECT
    'ethereum' AS blockchain,
    gauge AS address,
    COALESCE(v2pools.address, v3pools.address) AS pool_address,
    CAST(NULL AS VARBINARY) AS child_gauge_address,
    'eth:' || COALESCE(v2pools.name, v3pools.name) AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_ethereum' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'CappedLiquidityGaugeFactory_evt_GaugeCreated') }} evt
    INNER JOIN {{ source('balancer_ethereum', 'CappedLiquidityGaugeFactory_call_create') }} call ON call.call_tx_hash = evt.evt_tx_hash
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_ethereum') }} v2pools ON v2pools.address = call.pool
    LEFT JOIN {{ source('labels', 'balancer_v3_pools_ethereum') }} v3pools ON v3pools.address = call.pool

UNION ALL

SELECT
    'ethereum' AS blockchain,
    gauge_address AS address,
    BYTEARRAY_SUBSTRING(pool_id, 1,20) AS pool_address,
    CAST(NULL AS VARBINARY) AS child_gauge_address,
    'eth:' || project AS name,
    'balancer_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_gauges_ethereum' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer','single_recipient_gauges') }}
WHERE
    blockchain = 'ethereum'),

kill_unkill_1 AS(
    SELECT
        contract_address,
        call_block_time,
        'kill' AS action
    FROM {{ source('balancer_ethereum', 'LiquidityGauge_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'kill' AS action
    FROM {{ source('balancer_ethereum', 'LiquidityGaugeV5_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'LiquidityGauge_call_initialize') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'LiquidityGaugeV5_call_initialize') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('balancer_ethereum', 'CappedLiquidityGaugeV5_call_initialize') }}
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