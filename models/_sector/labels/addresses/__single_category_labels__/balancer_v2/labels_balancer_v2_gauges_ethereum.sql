{{config(

        alias = 'balancer_v2_gauges_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski", "viniabussafi"]\') }}')}}

WITH gauges AS(
SELECT
    'ethereum' AS blockchain,
    gauge AS address,
    pools.address AS pool_address,
    CAST(NULL AS VARBINARY) AS child_gauge_address,
    'eth:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_ethereum' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'LiquidityGaugeFactory_evt_GaugeCreated') }} gauge
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_ethereum') }} pools ON pools.address = gauge.pool

UNION ALL

SELECT
    'ethereum' AS blockchain,
    gauge AS address,
    pools.address AS pool_address,
    CAST(NULL AS VARBINARY) AS child_gauge_address,
    'eth:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_ethereum' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer_ethereum', 'CappedLiquidityGaugeFactory_evt_GaugeCreated') }} evt
    INNER JOIN {{ source('balancer_ethereum', 'CappedLiquidityGaugeFactory_call_create') }} call ON call.call_tx_hash = evt.evt_tx_hash
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_ethereum') }} pools ON pools.address = call.pool

UNION ALL

SELECT
    'ethereum' AS blockchain,
    gauge_address AS address,
    BYTEARRAY_SUBSTRING(pool_id, 1,20) AS pool_address,
    CAST(NULL AS VARBINARY) AS child_gauge_address,
    'eth:' || project AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_ethereum' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('balancer','single_recipient_gauges') }}
WHERE
    blockchain = 'ethereum'),

controller AS( --to allow filtering for active gauges only
SELECT
    c.evt_tx_hash,
    c.evt_index,
    c.evt_block_time,
    c.evt_block_number,
    c.addr AS address,
    ROW_NUMBER() OVER (PARTITION BY g.pool_address ORDER BY evt_block_time DESC) AS rn
FROM {{ source('balancer_ethereum', 'GaugeController_evt_NewGauge') }} c
INNER JOIN gauges g ON g.address = c.addr
)

    SELECT
          g.blockchain
         , g.address
         , g.pool_address
         , g.child_gauge_address
         , g.name
         , CASE WHEN c.rn = 1
            THEN 'active'
            ELSE 'inactive'
            END AS status
         , g.category
         , g.contributor
         , g.source
         , g.created_at
         , g.updated_at
         , g.model_name
         , g.label_type
    FROM gauges g
    INNER JOIN controller c ON g.address = c.address
