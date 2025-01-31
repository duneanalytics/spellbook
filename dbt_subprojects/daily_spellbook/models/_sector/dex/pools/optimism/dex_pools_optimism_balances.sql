{{
  config(
    schema = 'dex_pools_optimism',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_address', 'snapshot_day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.snapshot_day')]
  )
}}

WITH uniswap_v3_op_addresses AS (
  SELECT
    pool AS address,
    token0,
    token1,
    0x4200000000000000000000000000000000000042 AS token_address,
    creation_block_time AS creation_time,
    'uniswap' AS protocol_name,
    '3' AS version
  FROM 
    {{ source('uniswap_v3_optimism', 'pools') }}
  WHERE
    (token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042)
    {% if is_incremental() %}
    AND {{ incremental_predicate('creation_block_time') }}
    {% endif %}
),

uniswap_v2_op_addresses AS (
  SELECT
    pair AS address,
    token0,
    token1,
    0x4200000000000000000000000000000000000042 AS token_address,
    evt_block_time AS creation_time,
    'uniswap' AS protocol_name,
    '2' AS version
  FROM 
    {{ source('uniswap_v2_optimism', 'UniswapV2Factory_evt_PairCreated') }}
  WHERE
    (token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042)
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

velodrome_v2_op_addresses AS (
  SELECT
    pool AS address,
    token0,
    token1,
    0x4200000000000000000000000000000000000042 AS token_address,
    evt_block_time AS creation_time,
    'velodrome' AS protocol_name,
    '2' AS version
  FROM 
    {{ source('velodrome_v2_optimism', 'PoolFactory_evt_PoolCreated') }}
  WHERE
    (token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042)
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

velodrome_v2_cl_op_addresses AS (
  SELECT
    pool AS address,
    token0,
    token1,
    0x4200000000000000000000000000000000000042 AS token_address,
    evt_block_time AS creation_time,
    'velodrome' AS protocol_name,
    '2' AS version
  FROM 
    {{ source('velodrome_v2_optimism', 'CLFactory_evt_PoolCreated') }}
  WHERE
    (token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042)
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

velodrome_v1_op_addresses AS (
  SELECT
    pair AS address,
    token0,
    token1,
    0x4200000000000000000000000000000000000042 AS token_address,
    evt_block_time AS creation_time,
    'velodrome' AS protocol_name,
    '1' AS version
  FROM 
    {{ source('velodrome_optimism', 'PairFactory_evt_PairCreated') }}
  WHERE
    (token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042)
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

solidly_v3_op_addresses AS (
  SELECT
    pool AS address,
    token0,
    token1,
    0x4200000000000000000000000000000000000042 AS token_address,
    evt_block_time AS creation_time,
    'solidly' AS protocol_name,
    '3' AS version
  FROM 
    {{ source('solidly_v3_optimism', 'SolidlyV3Factory_evt_PoolCreated') }}
  WHERE
    (token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042)
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

openxswap_v1_op_addresses AS (
  SELECT
    pair AS address,
    token0,
    token1,
    0x4200000000000000000000000000000000000042 AS token_address,
    evt_block_time AS creation_time,
    'openxswap' AS protocol_name,
    '1' AS version
  FROM 
    {{ source('openxswap_optimism', 'UniswapV2Factory_evt_PairCreated') }}
  WHERE
    (token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042)
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

curve_op_addresses AS (
  SELECT
    pool AS address,
    NULL AS token0,
    NULL AS token1, 
    0x4200000000000000000000000000000000000042 AS token_address,
    NULL AS creation_time, 
    'curve' AS protocol_name,
    '1' AS version
  FROM 
    {{ source('curve_optimism', 'pools') }}
  WHERE
    token = 0x4200000000000000000000000000000000000042
),

op_addresses AS (
  SELECT * FROM uniswap_v3_op_addresses
  UNION ALL
  SELECT * FROM uniswap_v2_op_addresses
  UNION ALL
  SELECT * FROM velodrome_v2_op_addresses
  UNION ALL
  SELECT * FROM velodrome_v2_cl_op_addresses
  UNION ALL
  SELECT * FROM velodrome_v1_op_addresses
  UNION ALL
  SELECT * FROM solidly_v3_op_addresses
  UNION ALL
  SELECT * FROM openxswap_v1_op_addresses
  UNION ALL
  SELECT * FROM curve_op_addresses
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2021-11-11',
       address_token_list='op_addresses'
  ) }}
)

SELECT
  p.address AS pool_address,
  p.token0,
  p.token1,
  p.token_address AS token,
  p.creation_time,
  p.protocol_name,
  p.version,
  COALESCE(b.balance, 0) AS op_balance,
  COALESCE(b.day, current_date) AS snapshot_day
FROM 
  filtered_balances b
LEFT JOIN
  op_addresses p ON b.address = p.address
WHERE
  COALESCE(b.balance, 0) > 100