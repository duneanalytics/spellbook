{{ config(
    schema = 'swapr_gnosis',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    post_hook='{{ expose_spells(\'["gnosis"]\', "project", "swapr", \'["mlaegn"]\') }}'
) }}


with pair_creation as (
  SELECT 
    pair AS pool,
    token0,
    token1,
    evt_block_time AS creation_block_time,
    evt_block_number AS creation_block_number,
    contract_address
  FROM {{ source('swapr_gnosis', 'DXswapFactory_evt_PairCreated') }}
  {% if is_incremental() %}
  WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}
),


latest_fee as (
  SELECT 
    _pair AS pool,
    _swapFee,
    ROW_NUMBER() OVER (PARTITION BY _pair ORDER BY call_block_number DESC) AS rn
  FROM {{ source('swapr_gnosis', 'dxswapfactory_call_setswapfee') }}
  WHERE call_success = true
)

SELECT 
  'gnosis' AS blockchain,
  'swapr' AS project,
  'v2' AS version,
  pc.pool,
  COALESCE(lf._swapFee, 2500) AS fee, 
  pc.token0,
  pc.token1,
  pc.creation_block_time,
  pc.creation_block_number,
  pc.contract_address
FROM pair_creation pc
LEFT JOIN latest_fee lf
  ON pc.pool = lf.pool AND lf.rn = 1
;
