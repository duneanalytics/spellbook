{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'l2_revenue',
    
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
  date_trunc('day', t.block_time) AS day
  , 'zksync era' AS name
  , SUM((t.gas_used * t.gas_price)/POWER(10,18)) AS l2_rev
  , SUM(p.price * (t.gas_used * t.gas_price)/POWER(10,18)) AS l2_rev_usd
FROM {{ source('zksync','transactions') }} t
INNER JOIN {{ source('prices','usd') }} p
  ON p.minute = date_trunc('minute', t.block_time)
  AND p.blockchain is null
  AND p.symbol = 'ETH'
  {% if is_incremental() %}
  AND {{incremental_predicate('p.minute')}}
  {% endif %}
WHERE
  1 = 1
  {% if is_incremental() %}
  AND {{incremental_predicate('t.block_time')}}
  {% else %}
  AND t.block_time >= timestamp '2022-01-01'
  {% endif %}
GROUP BY 1,2

UNION ALL

SELECT
  date_trunc('day', t.block_time) AS day
  , 'arbitrum' AS name
  , SUM((t.gas_used * t.effective_gas_price)/POWER(10,18)) AS l2_rev
  , SUM(p.price * (t.gas_used * t.effective_gas_price)/POWER(10,18)) AS l2_rev_usd
FROM {{ source('arbitrum','transactions') }} t
INNER JOIN {{ source('prices','usd') }} p
  ON p.minute = date_trunc('minute', t.block_time)
  AND p.blockchain is null
  AND p.symbol = 'ETH'
  {% if is_incremental() %}
  AND {{incremental_predicate('p.minute')}}
  {% endif %}
WHERE
  1 = 1
  {% if is_incremental() %}
  AND {{incremental_predicate('t.block_time')}}
  {% else %}
  AND t.block_time >= timestamp '2022-01-01'
  {% endif %}
GROUP BY 1,2

UNION ALL

SELECT
  date_trunc('day', t.block_time) AS day
  , 'op mainnet' AS name
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
  {% if is_incremental() %}
  AND {{incremental_predicate('p.minute')}}
  {% endif %}
WHERE
  1 = 1
  {% if is_incremental() %}
  AND {{incremental_predicate('t.block_time')}}
  {% else %}
  AND t.block_time > timestamp '2023-06-06 16:11' --when bedrock upgrade happened
  {% endif %}
GROUP BY 1,2

--since the below is a hardcoded historical date range, only run on full refresh
{% if not is_incremental() %}
UNION ALL

SELECT
  date_trunc('day', t.block_time) AS day
  , 'op mainnet (ovm2)' AS name
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
WHERE t.block_time >= timestamp '2022-01-01'
  AND t.block_time <= timestamp '2023-06-06 18:03'
GROUP BY 1,2
{% endif %}

UNION ALL

SELECT
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
  {% if is_incremental() %}
  AND {{incremental_predicate('p.minute')}}
  {% endif %}
WHERE
  1 = 1
  {% if is_incremental() %}
  AND {{incremental_predicate('t.block_time')}}
  {% else %}
  AND t.block_time >= timestamp '2022-01-01'
  {% endif %}
GROUP BY 1,2

UNION ALL

SELECT
  date_trunc('day', t.block_time) AS day
  , 'zora' AS name
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
FROM {{ source('zora','transactions') }} t
INNER JOIN {{ source('prices','usd') }} p
  ON p.minute = date_trunc('minute', t.block_time)
  AND p.blockchain is null
  AND p.symbol = 'ETH'
  {% if is_incremental() %}
  AND {{incremental_predicate('p.minute')}}
  {% endif %}
WHERE
  1 = 1
  {% if is_incremental() %}
  AND {{incremental_predicate('t.block_time')}}
  {% else %}
  AND t.block_time >= timestamp '2023-06-12' --when zora network launched
  {% endif %}
GROUP BY 1,2

UNION ALL

SELECT
  date_trunc('day', t.block_time) AS day
  , 'scroll' AS name
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
FROM {{ source('scroll','transactions') }} t
INNER JOIN {{ source('prices','usd') }} p
  ON p.minute = date_trunc('minute', t.block_time)
  AND p.blockchain is null
  AND p.symbol = 'ETH'
  {% if is_incremental() %}
  AND {{incremental_predicate('p.minute')}}
  {% endif %}
WHERE
  1 = 1
  {% if is_incremental() %}
  AND {{incremental_predicate('t.block_time')}}
  {% else %}
  AND t.block_time >= timestamp '2023-10-10' --when scroll launched
  {% endif %}
GROUP BY 1,2