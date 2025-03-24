{{config(
        alias = 'beets_gauges_sonic',
        post_hook='{{ expose_spells(\'["sonic"]\',
                                    "sector",
                                    "labels",
                                    \'["viniabussafi"]\') }}')}}

WITH gauges AS(
SELECT distinct
    'sonic' AS blockchain,
    gauge.gauge AS address,
    pools.address AS pool_address,
    'S:' || pools.name AS name,
    'beets_gauges' AS category,
    'beets' AS contributor,
    'query' AS source,
    TIMESTAMP '2024-12-01'  AS created_at,
    NOW() AS updated_at,
    'beets_gauges_sonic' AS model_name,
    'identifier' AS label_type
FROM
    {{ source('beethoven_x_v2_sonic', 'ChildChainGaugeFactory_evt_GaugeCreated') }} gauge
    INNER JOIN {{ source('beethoven_x_v2_sonic', 'ChildLiquidityGauge_call_initialize') }} call ON gauge.gauge = call.contract_address
    LEFT JOIN {{ ref('labels_beets_pools_sonic') }} pools ON pools.address = call._lp_token
WHERE pools.name IS NOT NULL
AND call.call_success),

kill_unkill_1 AS(
    SELECT
        contract_address,
        call_block_time,
        'kill' AS action
    FROM {{ source('beethoven_x_v2_sonic', 'ChildLiquidityGauge_call_killGauge') }}
    WHERE call_success

    UNION ALL

    SELECT
        contract_address,
        call_block_time,
        'unkill' AS action
    FROM {{ source('beethoven_x_v2_sonic', 'ChildLiquidityGauge_call_initialize') }}
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