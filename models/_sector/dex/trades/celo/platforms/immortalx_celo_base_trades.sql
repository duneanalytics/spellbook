{{
  config(
    schema = 'immortalx_celo',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

select 
  'celo' as blockchain,
  'immortalx' as project,
  '1' as version,
  cast(date_trunc('month', t.evt_block_time) as date) as block_month,
  cast(t.evt_block_time as date) as block_date,
  t.evt_block_time as block_time,
  t.evt_block_number as block_number,
  t.user as taker,
  cast(null as varbinary) as maker,
  t.returnAmount as token_bought_amount_raw,
  cast(t.decreaseMargin * 1e18 / t.decreasePrice as uint256) as token_sold_amount_raw,
  0x765DE816845861e75A25fCA122bb6898B8B1282a as token_bought_address, -- cUSD
  case
    when t.marketId = 1 then 0xd629eb00deced2a080b7ec630ef6ac117e614f1b -- BTC/USD position
    when t.marketId = 2 then 0x66803fb87abd4aac3cbb3fad7c3aa01f6f3fb207 -- ETH/USD position
    when t.marketId = 3 then 0x471EcE3750Da237f93B8E339c536989b8978a438 -- CELO/USD position
    else 0x0000000000000000000000000000000000000000 -- unknown position
  end as token_sold_address,
  t.contract_address as project_contract_address,
  t.evt_tx_hash as tx_hash,
  t.evt_index
from {{ source('immortalx_celo', 'Dex_evt_DecreasePosition') }} t
where t.returnAmount > 0
  {% if is_incremental() %}
  and {{ incremental_predicate('t.evt_block_time') }}
  {% endif %}
