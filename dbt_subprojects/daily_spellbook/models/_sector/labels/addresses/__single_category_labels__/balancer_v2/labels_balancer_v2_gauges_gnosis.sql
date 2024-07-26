{{config(
        
        alias = 'balancer_v2_gauges_gnosis',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "labels",
                                    \'["viniabussafi"]\') }}')}}

WITH gauges AS(
SELECT distinct
    'gnosis' AS blockchain,
    call.output_0 AS address,
    pools.address AS pool_address,
    child.output_0 AS child_gauge_address,    
    'gno:' || pools.name AS name,
    'balancer_v2_gauges' AS category,
    'balancerlabs' AS contributor,
    'query' AS source,
    TIMESTAMP '2022-01-13'  AS created_at,
    NOW() AS updated_at,
    'balancer_v2_gauges_gnosis' AS model_name,
    'identifier' AS label_type
FROM {{ source('balancer_ethereum', 'GnosisRootGaugeFactory_call_create') }} call
    LEFT JOIN {{ source('balancer_gnosis', 'ChildChainGaugeFactory_call_create') }} child ON child.output_0 = call.recipient
    LEFT JOIN {{ source('labels', 'balancer_v2_pools_gnosis') }} pools ON pools.address = child.pool),

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
