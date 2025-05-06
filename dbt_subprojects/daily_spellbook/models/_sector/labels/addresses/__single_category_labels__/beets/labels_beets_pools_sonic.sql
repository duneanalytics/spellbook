{{config(
  alias = 'beets_pools_sonic',
  post_hook = '{{ expose_spells(\'["sonic"]\',
                               "sector",
                               "labels",
                               \'["viniabussafi"]\') }}'
)}}

WITH v2_pools AS(
  WITH pools AS (
  SELECT
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type,
    pool_name
  FROM (
    SELECT
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type,
      cc.name AS pool_name
    FROM {{ source('beethoven_x_v2_sonic', 'Vault_evt_PoolRegistered') }} c
    INNER JOIN {{ source('beethoven_x_v2_sonic', 'WeightedPoolFactory_call_create') }} cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip


  UNION ALL

  SELECT
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'stable' AS pool_type,
    cc.name AS pool_name
  FROM {{ source('beethoven_x_v2_sonic', 'Vault_evt_PoolRegistered') }} c
  INNER JOIN {{ source('beethoven_x_v2_sonic', 'ComposableStablePoolFactory_call_create') }} cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'ECLP' AS pool_type,
    cc.name
  FROM {{ source('beethoven_x_v2_sonic', 'Vault_evt_PoolRegistered') }} c
  INNER JOIN {{ source('gyroscope_sonic', 'GyroECLPPoolFactory_call_create') }} cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

),

settings AS (
  SELECT
    pool_id,
    coalesce(t.symbol, '?') AS token_symbol,
    normalized_weight,
    p.symbol AS pool_symbol,
    p.pool_type,
    p.pool_name
  FROM pools p
  LEFT JOIN {{ source('tokens', 'erc20') }} t ON p.token_address = t.contract_address
  AND t.blockchain = 'sonic'
)

SELECT 
  'sonic' AS blockchain,
  bytearray_substring(pool_id, 1, 20) AS address,
  CASE WHEN pool_type IN ('stable', 'ECLP') 
  THEN lower(pool_symbol)
    ELSE lower(concat(array_join(array_agg(token_symbol ORDER BY token_symbol), '/'), ' ', 
    array_join(array_agg(cast(norm_weight AS varchar) ORDER BY token_symbol), '/')))
  END AS name,
  pool_name AS poolname,
  pool_type,
  '2' AS version,
  'beets_v2_pool' AS category,
  'beets' AS contributor,
  'query' AS source,
  TIMESTAMP'2024-12-15 00:00' AS created_at,
  now() AS updated_at,
  'beets_pools_sonic' AS model_name,
  'identifier' AS label_type
FROM (
  SELECT
    s1.pool_id,
    token_symbol,
    pool_symbol,
    cast(100 * normalized_weight AS integer) AS norm_weight,
    pool_type,
    pool_name
  FROM settings s1
  GROUP BY s1.pool_id, token_symbol, pool_symbol, normalized_weight, pool_type, pool_name
) s
GROUP BY pool_id, pool_symbol, pool_type, pool_name
ORDER BY 1),

v3_pools AS(
  WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(FROM_HEX(json_extract_scalar(token, '$.token')) ORDER BY token_index) AS tokens 
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM {{ source('beethoven_x_v3_sonic', 'Vault_evt_PoolRegistered') }}
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1
    ),

    pools AS (
      SELECT
        pool_id,
        zip.tokens AS token_address,
        zip.weights / POWER(10, 18) AS normalized_weight,
        symbol,
        pool_type
      FROM (
        SELECT
          c.pool AS pool_id,
          t.tokens,
          w.weights,
          cc.symbol,
          'weighted' AS pool_type
        FROM token_data c
        INNER JOIN {{ source('beethoven_x_v3_sonic', 'WeightedPoolFactory_call_create') }} cc
        ON c.pool = cc.output_pool
        CROSS JOIN UNNEST(c.tokens) WITH ORDINALITY t(tokens, pos)
        CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos)
        WHERE t.pos = w.pos

      UNION ALL

      SELECT
        c.pool AS pool_id,
        t.tokens,
        0 AS weights,
        cc.symbol,
        'stable' AS pool_type
      FROM token_data c
      INNER JOIN {{ source('beethoven_x_v3_sonic', 'StablePoolFactory_call_create') }} cc
        ON c.pool = cc.output_pool
      CROSS JOIN UNNEST(c.tokens) AS t(tokens)

      UNION ALL

      SELECT
        c.pool AS pool_id,
        t.tokens,
        0 AS weights,
        cc.symbol,
        'stable' AS pool_type
      FROM token_data c
      INNER JOIN {{ source('beethoven_x_v3_sonic', 'GyroECLPPoolFactory_call_create') }} cc
        ON c.pool = cc.output_pool
      CROSS JOIN UNNEST(c.tokens) AS t(tokens)
    ) zip 
          ),

    settings AS (
      SELECT
        pool_id,
        coalesce(t.symbol, '?') AS token_symbol,
        normalized_weight,
        p.symbol AS pool_symbol,
        p.pool_type
      FROM pools p
      LEFT JOIN {{ source('tokens', 'erc20') }} t ON p.token_address = t.contract_address
      AND t.blockchain = 'sonic'
    )

SELECT 
  'sonic' AS blockchain,
  bytearray_substring(pool_id, 1, 20) AS address,
  CASE WHEN pool_type IN ('stable', 'ECLP') 
  THEN lower(pool_symbol)
    ELSE lower(concat(array_join(array_agg(token_symbol ORDER BY token_symbol), '/'), ' ', 
    array_join(array_agg(cast(norm_weight AS varchar) ORDER BY token_symbol), '/')))
  END AS name,
  pool_type,
  '3' AS version,
  'beets_v3_pool' AS category,
  'beets' AS contributor,
  'query' AS source,
  TIMESTAMP'2024-12-15 00:00' AS created_at,
  now() AS updated_at,
  'beets_pools_sonic' AS model_name,
  'identifier' AS label_type
FROM (
  SELECT
    s1.pool_id,
    token_symbol,
    pool_symbol,
    cast(100 * normalized_weight AS integer) AS norm_weight,
    pool_type
  FROM settings s1
  GROUP BY s1.pool_id, token_symbol, pool_symbol, normalized_weight, pool_type
) s
GROUP BY pool_id, pool_symbol, pool_type
ORDER BY 1
)

SELECT 
  blockchain,
  address,
  name,
  pool_type,
  version,
  category,
  contributor,
  source,
  created_at,
  updated_at,
  model_name,
  label_type
FROM v2_pools

UNION

SELECT 
  blockchain,
  address,
  name,
  pool_type,
  version,
  category,
  contributor,
  source,
  created_at,
  updated_at,
  model_name,
  label_type
FROM v3_pools