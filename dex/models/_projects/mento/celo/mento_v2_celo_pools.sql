{{
  config(
    schema = 'mento_v2_celo',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['contract_address', 'pool', 'creation_block_time']
  )
}}

select
  'celo' as blockchain,
  'mento' as project,
  '2' as version,
  ec.exchangeId as pool,
  cast(null as decimal(38,1)) as fee,
  ec.asset0 as token0,
  ec.asset1 as token1,
  ec.evt_block_time as creation_block_time,
  ec.evt_block_number as creation_block_number,
  ec.contract_address
from {{ source('mento_celo', 'BiPoolManager_evt_ExchangeCreated') }} ec
  left join {{ source('mento_celo', 'BiPoolManager_evt_ExchangeDestroyed') }} ed on ec.exchangeId = ed.exchangeId
where ed.exchangeId is null
{% if is_incremental() %}
  and {{ incremental_predicate('ec.evt_block_time') }}
{% endif %}
