{{
  config(
    schema = 'mento_v1_celo',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{% set CELO = '0x471EcE3750Da237f93B8E339c536989b8978a438' %}
{% set cUSD = '0x765DE816845861e75A25fCA122bb6898B8B1282a' %}
{% set cEUR = '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73' %}
{% set cREAL = '0xe8537a3d056da446677b9e9d6c5db704eaab4787' %}

--Mento v1

with base_trades as (
  select
    t.evt_block_time as block_time,
    t.evt_block_number as block_number,
    t.exchanger as taker,
    cast(null as varbinary) as maker,
    t.buyAmount as token_bought_amount_raw,
    t.sellAmount as token_sold_amount_raw,
    cast(null as double) as amount_usd,
    if(t.soldGold, {{cUSD}}, {{CELO}}) as token_bought_address,
    if(t.soldGold, {{CELO}}, {{cUSD}}) as token_sold_address,
    t.contract_address as project_contract_address,
    t.evt_tx_hash as tx_hash,
    t.evt_index
  from {{ source('mento_celo', 'Exchange_evt_Exchanged') }} t
  {% if is_incremental() %}
  where {{ incremental_predicate('t.evt_block_time') }}
  {% endif %}

  union all

  select
    t.evt_block_time as block_time,
    t.evt_block_number as block_number,
    t.exchanger as taker,
    cast(null as varbinary) as maker,
    t.buyAmount as token_bought_amount_raw,
    t.sellAmount as token_sold_amount_raw,
    cast(null as double) as amount_usd,
    if(t.soldGold, {{cEUR}}, {{CELO}}) as token_bought_address,
    if(t.soldGold, {{CELO}}, {{cEUR}}) as token_sold_address,
    t.contract_address as project_contract_address,
    t.evt_tx_hash as tx_hash,
    t.evt_index
  from {{ source('mento_celo', 'ExchangeEUR_evt_Exchanged') }} t
  {% if is_incremental() %}
  where {{ incremental_predicate('t.evt_block_time') }}
  {% endif %}

  union all

  select
    t.evt_block_time as block_time,
    t.evt_block_number as block_number,
    t.exchanger as taker,
    cast(null as varbinary) as maker,
    t.buyAmount as token_bought_amount_raw,
    t.sellAmount as token_sold_amount_raw,
    cast(null as double) as amount_usd,
    if(t.soldGold, {{cREAL}}, {{CELO}}) as token_bought_address,
    if(t.soldGold, {{CELO}}, {{cREAL}}) as token_sold_address,
    t.contract_address as project_contract_address,
    t.evt_tx_hash as tx_hash,
    t.evt_index
  from {{ source('mento_celo', 'ExchangeBRL_evt_Exchanged') }} t
  {% if is_incremental() %}
  where {{ incremental_predicate('t.evt_block_time') }}
  {% endif %}
)

select
  'celo' as blockchain,
  'mento' as project,
  '1' as version,
  cast(date_trunc('month', block_time) as date) as block_month,
  cast(block_time as date) as block_date,
  block_time,
  block_number,
  token_bought_amount_raw,
  token_sold_amount_raw,
  token_bought_address,
  token_sold_address,
  taker,
  maker,
  project_contract_address,
  tx_hash,
  evt_index
from base_trades
