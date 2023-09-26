{{
  config(
    tags = ['dunesql'],
    schema = 'mento_celo',
    alias = alias('pools_v2'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['contract_address', 'exchange_id', 'block_time_created', 'is_active']
  )
}}

select
  'celo' as blockchain,
  'mento' as project,
  'v2' as version,
  ec.contract_address, -- BiPoolManager
  concat(erc20a.symbol, '/', erc20b.symbol) as token_pair,
  ec.asset0,
  ec.asset1,
  ec.exchangeId as exchange_id,
  case when ed.exchangeId is not null then 0 else 1 end as is_active,
  ec.evt_block_time as block_time_created,
  ec.evt_block_number as block_number_created,
  ed.evt_block_time as block_time_destroyed,
  ed.evt_block_number as block_number_destroyed
from {{ source('mento_celo', 'BiPoolManager_evt_ExchangeCreated') }} ec
  left join {{ source('mento_celo', 'BiPoolManager_evt_ExchangeDestroyed') }} ed on ec.exchangeId = ed.exchangeId and ec.evt_tx_hash = ed.evt_tx_hash
  left join {{ ref('tokens_erc20') }} erc20a on ec.asset0 = erc20a.contract_address and erc20a.blockchain = 'celo'
  left join {{ ref('tokens_erc20') }} erc20b on ec.asset1 = erc20b.contract_address and erc20b.blockchain = 'celo'
{% if is_incremental() %}
where ec.evt_block_time >= date_trunc('day', now() - interval '7' day)
   or ed.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
