{{ config(
    schema = 'rollup_economics_ethereum',
    alias = alias('l2_revenue'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'name'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable"]\') }}'
)}}


SELECT
date_trunc('day', tr.block_time) as day
, 'zksync era' AS name
, SUM(cast(tr.value as double)/1e18) as l2_rev
, SUM(p.price * cast(tr.value as double)/1e18) as l2_rev_usd
FROM {{ source('ethereum','traces') }} tr
INNER JOIN {{ source('prices','usd') }} p
  ON p.minute = date_trunc('minute', tr.block_time)
  AND p.blockchain is null
  AND p.symbol = 'ETH'
  AND tr.success=true
  AND tr.type='call'
  AND (tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR tr.call_type IS null)
  AND (tr."from" = 0xfeee860e7aae671124e9a4e61139f3a5085dfeee
      OR tr."from" = 0xa9232040bf0e0aea2578a5b2243f2916dbfc0a69
    )
  AND cast(tr.value as double)/1e18 > 0
  AND tr.block_time >= timestamp '2023-02-01'
  {% if is_incremental() %}
  AND tr.block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}
  GROUP BY 1,2

UNION ALL SELECT
date_trunc('day', t.block_time) AS day
, 'arbitrum' AS name
, SUM((t.gas_used * t.effective_gas_price)/POWER(10,18)) AS l2_rev
, SUM(p.price * (t.gas_used * t.effective_gas_price)/POWER(10,18)) AS l2_rev_usd
FROM {{ source('arbitrum','transactions') }} t
INNER JOIN {{ source('prices','usd') }} p
  ON p.minute = date_trunc('minute', t.block_time)
  AND p.blockchain is null
  AND p.symbol = 'ETH'
  AND t.block_time >= timestamp '2022-01-01'
  {% if is_incremental() %}
  AND t.block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}
  GROUP BY 1,2

UNION ALL SELECT
date_trunc('day', t.block_time) AS day
, 'optimism' AS name
, SUM(
  CASE WHEN cast(t.gas_price as double) = cast(0 as double) THEN 0
  ELSE (l1_fee + (cast(t.gas_used as double) * cast(t.gas_price as double))) /POWER(10,18)
  END
) AS l2_rev
, SUM(
  p.price *
  (CASE WHEN cast(t.gas_price as double) = cast(0 as double) THEN 0
  ELSE (l1_fee + (cast(t.gas_used as double) * cast(t.gas_price as double))) /POWER(10,18)
  END)
) AS l2_rev_usd
FROM {{ source('optimism','transactions') }} t
INNER JOIN {{ source('prices','usd') }} p
  ON p.minute = date_trunc('minute', t.block_time)
  AND p.blockchain is null
  AND p.symbol = 'ETH'
  AND t.block_time >= timestamp '2022-01-01'
  {% if is_incremental() %}
  AND t.block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}
  GROUP BY 1,2

UNION ALL SELECT
date_trunc('day', t.block_time) AS day
, 'base' AS name
, SUM(
  CASE WHEN cast(t.gas_price as double) = cast(0 as double) THEN 0
  ELSE (l1_fee + (cast(t.gas_used as double) * cast(t.gas_price as double))) /POWER(10,18)
  END
) AS l2_rev
, SUM(
  p.price *
  (CASE WHEN cast(t.gas_price as double) = cast(0 as double) THEN 0
  ELSE (l1_fee + (cast(t.gas_used as double) * cast(t.gas_price as double))) /POWER(10,18)
  END)
) AS l2_rev_usd
FROM {{ source('base','transactions') }} t
INNER JOIN {{ source('prices','usd') }} p
  ON p.minute = date_trunc('minute', t.block_time)
  AND p.blockchain is null
  AND p.symbol = 'ETH'
  AND t.block_time >= timestamp '2022-01-01'
  {% if is_incremental() %}
  AND t.block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}
  GROUP BY 1,2
