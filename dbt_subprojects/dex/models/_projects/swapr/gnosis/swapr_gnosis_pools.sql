{{ config(
    schema = 'swapr_gnosis',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool','creation_block_number','creation_block_time']
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
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

fee_updates as (
  SELECT 
    _pair AS pool,
    _swapFee AS fee,
    call_block_time AS creation_block_time,
    call_block_number AS creation_block_number
  FROM {{ source('swapr_gnosis', 'DXswapFactory_call_setSwapFee') }}
  WHERE call_success = true
  {% if is_incremental() %}
  AND {{ incremental_predicate('call_block_time') }}
  {% endif %}
)

select 
  'gnosis' AS blockchain,
  'swapr' AS project,
  'v2' AS version,
  pc.pool,
  2500 AS fee,  
  pc.token0,
  pc.token1,
  pc.creation_block_time,
  pc.creation_block_number,
  pc.contract_address
from pair_creation pc

union all

select 
  'gnosis' AS blockchain,
  'swapr' AS project,
  'v2' AS version,
  pc.pool,
  fu.fee,
  pc.token0,
  pc.token1,
  fu.creation_block_time,  
  fu.creation_block_number, 
  pc.contract_address
from fee_updates fu
join pair_creation pc
  on fu.pool = pc.pool