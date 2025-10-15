{{ config(
    schema = 'thorchain_silver',
    alias = 'prices',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'symbol', 'contract_address'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'prices']
) }}

-- Normalize block log once (height + timestamp)
WITH blk AS (
  SELECT
    -- raw block_log has nanoseconds in 'timestamp'
    CAST(from_unixtime(CAST(timestamp/1e9 AS bigint)) AS timestamp) AS block_time,
    height                                                       AS block_id
  FROM {{ source('thorchain','block_log') }}
  WHERE CAST(from_unixtime(CAST(timestamp/1e9 AS bigint)) AS timestamp) >= current_date - interval '15' day
),

-- RUNE price (has block_time; bring in height)
rune_prices AS (
  SELECT
      r.block_time,
      DATE(r.block_time)                               AS block_date,
      date_trunc('month', r.block_time)                AS block_month,
      b.block_id,                                      -- << height for downstream
      r.rune_price_usd                                 AS price,
      'RUNE'                                           AS symbol,
      'thorchain'                                      AS blockchain,
      CAST(X'' AS varbinary)                           AS contract_address
  FROM {{ ref('thorchain_silver_rune_price') }} r
  JOIN blk b
    ON r.block_time = b.block_time
  {% if is_incremental() %}
    WHERE {{ incremental_predicate('r.block_time') }}
  {% endif %}
),

-- External asset prices from global prices.usd (minute grain, no reliable height)
external_prices AS (
  SELECT
      p.minute                                         AS block_time,
      DATE(p.minute)                                   AS block_date,
      date_trunc('month', p.minute)                    AS block_month,
      CAST(NULL AS bigint)                             AS block_id,     -- height unknown here
      p.price,
      p.symbol,
      'thorchain'                                      AS blockchain,
      CAST(p.contract_address AS varbinary)            AS contract_address
  FROM {{ source('prices','usd') }} p
  WHERE p.blockchain = 'thorchain'
    AND p.price IS NOT NULL
    AND p.symbol <> 'RUNE'
    AND p.minute >= current_date - interval '15' day
)

SELECT block_time, block_date, block_month, block_id, price, symbol, blockchain, contract_address
FROM rune_prices

UNION ALL

SELECT block_time, block_date, block_month, block_id, price, symbol, blockchain, contract_address
FROM external_prices
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
