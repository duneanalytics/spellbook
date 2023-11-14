{{
  config(
    schema = 'mento_celo',
    alias = 'pools_v2',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['contract_address', 'pool', 'creation_block_time']
  )
}}

select
  'celo' as blockchain,
  'mento' as project,
  'v2' as version,
  ec.contract_address as pool,
  cast(null as decimal(38,1)) as fee,
  ec.asset0 as token0,
  ec.asset1 as token1,
  ec.evt_block_time as creation_block_time,
  ec.evt_block_number as creation_block_number,
  ec.contract_address
from {{ source('mento_celo', 'BiPoolManager_evt_ExchangeCreated') }} ec
{% if is_incremental() %}
where {{ incremental_predicate('ec.evt_block_time') }}
{% endif %}
