{{config(
  schema = 'labels',
  alias = 'balancer_v2_pools_optimism',
  post_hook = '{{ expose_spells(\'["optimism"]\',
                               "sector",
                               "labels",
                               \'["balancerlabs"]\') }}'
)}}

WITH pools AS (
  SELECT
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type
    FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
    INNER JOIN {{ source('balancer_v2_optimism', 'WeightedPoolFactory_call_create') }} cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL  
    
    SELECT
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type
    FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
    INNER JOIN {{ source('balancer_v2_optimism', 'WeightedPoolFactory_call_create') }} cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL

  SELECT
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type
    FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
    INNER JOIN {{ source('balancer_v2_optimism', 'WeightedPoolV2Factory_call_create') }} cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL

  SELECT
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type
    FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
    INNER JOIN {{ source('balancer_v2_optimism', 'WeightedPool2TokensFactory_call_create') }} cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL

  SELECT
    c.poolId AS pool_id,
    t.tokens,
    0 AS weights,
    cc.symbol,
    'stable' AS pool_type
  FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
  INNER JOIN {{ source('balancer_v2_optimism', 'StablePoolFactory_call_create') }} cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'stable' AS pool_type
  FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
  INNER JOIN {{ source('balancer_v2_optimism', 'MetaStablePoolFactory_call_create') }} cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'LBP' AS pool_type
  FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
  INNER JOIN {{ source('balancer_v2_optimism', 'NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create') }} cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'stable' AS pool_type
  FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
  INNER JOIN {{ source('balancer_v2_optimism', 'ComposableStablePoolFactory_call_create') }} cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT
    c.poolId AS pool_id,
    element AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'linear' AS pool_type
  FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
  INNER JOIN {{ source('balancer_v2_optimism', 'AaveLinearPoolFactory_call_create') }} cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t (element)

  UNION ALL

  SELECT
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'ECLP' AS pool_type
  FROM {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} c
  INNER JOIN {{ source('gyroscope_optimism', 'GyroECLPPoolFactory_call_create') }} cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens_array_binary) AS t(tokens)
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
  AND t.blockchain = 'optimism'
)

SELECT 
  'optimism' AS blockchain,
  bytearray_substring(pool_id, 1, 20) AS address,
  CASE WHEN pool_type IN ('stable', 'linear', 'LBP', 'ECLP') 
    THEN LOWER(pool_symbol)
    ELSE lower(concat(array_join(array_agg(token_symbol ORDER BY token_symbol), '/'), ' ', 
    array_join(array_agg(cast(norm_weight AS varchar) ORDER BY token_symbol), '/')))
  END AS name,
  pool_type,
  'balancer_v2_pool' AS category,
  'balancerlabs' AS contributor,
  'query' AS source,
  TIMESTAMP'2022-12-23 00:00' AS created_at,
  now() AS updated_at,
  'balancer_v2_pools_optimism' AS model_name,
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
