{{ config(
    schema = 'swaprv3_gnosis',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool', 'creation_block_time']
) }}

WITH pool_creation AS (
  SELECT 
    pool AS pool,
    token0,
    token1,
    evt_block_time AS creation_block_time,
    evt_block_number AS creation_block_number,
    contract_address AS factory_address
  FROM {{ source('swaprv3_gnosis', 'SwaprV3Factory_evt_Pool') }}
  {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),
fee_updates AS (
  SELECT 
    contract_address AS pool, 
    fee,
    evt_block_time AS creation_block_time,
    evt_block_number AS creation_block_number
  FROM {{ source('swaprv3_gnosis', 'AlgebraPool_evt_Fee') }}
  {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}
)

SELECT 
  'gnosis' AS blockchain,
  'swapr' AS project,
  'v3' AS version,
  pc.pool,
  100 AS fee,  -- default fee for swaprv3 pools from the factory
  pc.token0,
  pc.token1,
  pc.creation_block_time,
  pc.creation_block_number,
  pc.factory_address AS contract_address
FROM pool_creation pc

UNION ALL

SELECT 
  'gnosis' AS blockchain,
  'swaprv3' AS project,
  'v3' AS version,
  pc.pool,
  fu.fee,
  pc.token0,
  pc.token1,
  fu.creation_block_time,  
  fu.creation_block_number, 
  pc.factory_address AS contract_address
FROM fee_updates fu
JOIN pool_creation pc
  ON fu.pool = pc.pool