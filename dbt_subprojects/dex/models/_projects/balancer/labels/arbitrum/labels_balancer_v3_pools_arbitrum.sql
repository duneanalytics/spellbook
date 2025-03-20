{{config(
  schema = 'labels',
  alias = 'balancer_v3_pools_arbitrum',
  post_hook = '{{ expose_spells(\'["arbitrum"]\',
                               "sector",
                               "labels",
                               \'["viniabussafi"]\') }}'
)}}

WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(FROM_HEX(json_extract_scalar(token, '$.token')) ORDER BY token_index) AS tokens 
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM {{ source('balancer_v3_arbitrum', 'Vault_evt_PoolRegistered') }}
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
        INNER JOIN {{ source('balancer_v3_arbitrum', 'WeightedPoolFactory_call_create') }} cc
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
      INNER JOIN {{ source('balancer_v3_arbitrum', 'StablePoolFactory_call_create') }} cc
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
      INNER JOIN {{ source('balancer_v3_arbitrum', 'StableSurgePoolFactory_call_create') }} cc
        ON c.pool = cc.output_pool
      CROSS JOIN UNNEST(c.tokens) AS t(tokens)

      UNION ALL

      SELECT
        c.pool AS pool_id,
        t.tokens,
        0 AS weights,
        cc.symbol,
        'LBP' AS pool_type
      FROM token_data c
      INNER JOIN {{ source('balancer_v3_arbitrum', 'LBPoolFactory_call_create') }} cc
        ON c.pool = cc.output_pool
      CROSS JOIN UNNEST(c.tokens) AS t(tokens)

      UNION ALL

      SELECT
        c.pool AS pool_id,
        t.tokens AS token_address,
        0 AS normalized_weight,
        cc.symbol,
        'ECLP' AS pool_type
      FROM token_data c
      INNER JOIN {{ source('balancer_v3_arbitrum', 'GyroECLPPoolFactory_call_create') }} cc
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
      AND t.blockchain = 'arbitrum'
    )

SELECT 
  'arbitrum' AS blockchain,
  bytearray_substring(pool_id, 1, 20) AS address,
  CASE WHEN pool_type IN ('stable', 'LBP', 'ECLP') 
  THEN lower(pool_symbol)
    ELSE lower(concat(array_join(array_agg(token_symbol ORDER BY token_symbol), '/'), ' ', 
    array_join(array_agg(cast(norm_weight AS varchar) ORDER BY token_symbol), '/')))
  END AS name,
  pool_type,
  'balancer_v3_pool' AS category,
  'balancerlabs' AS contributor,
  'query' AS source,
  TIMESTAMP'2024-12-01 00:00' AS created_at,
  now() AS updated_at,
  'balancer_v3_pools_arbitrum' AS model_name,
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