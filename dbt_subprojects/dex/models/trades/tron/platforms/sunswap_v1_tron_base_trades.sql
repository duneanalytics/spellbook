{{
  config(
    schema = 'sunswap_v1_tron',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

{% set tron_native_address = '0x0000000000000000000000000000000000000000' %}

with dexs as (
  select
    s.evt_block_number as block_number,
    s.evt_block_time as block_time,
    s.buyer as taker,
    cast(null as varbinary) as maker,
    case
      when s.swap_type = 'token_purchase' then s.token_amount
      else s.trx_amount
    end as token_bought_amount_raw,
    case
      when s.swap_type = 'token_purchase' then s.trx_amount
      else s.token_amount
    end as token_sold_amount_raw,
    case
      when s.swap_type = 'token_purchase' then e.token
      else {{ tron_native_address }}
    end as token_bought_address,
    case
      when s.swap_type = 'token_purchase' then {{ tron_native_address }}
      else e.token
    end as token_sold_address,
    s.contract_address as project_contract_address,
    s.evt_tx_hash as tx_hash,
    s.evt_index
  from {{ ref('sunswap_v1_tron_swap_events') }} as s
  inner join {{ source('sunswap_v1_tron', 'justswapfactory_evt_newexchange') }} as e
    on e.exchange = s.contract_address
  {% if is_incremental() %}
    where {{ incremental_predicate('s.evt_block_time') }}
  {% endif %}
)

select
  'tron' as blockchain,
  'sunswap' as project,
  '1' as version,
  cast(date_trunc('month', dexs.block_time) as date) as block_month,
  cast(date_trunc('day', dexs.block_time) as date) as block_date,
  dexs.block_time,
  dexs.block_number,
  dexs.token_bought_amount_raw,
  dexs.token_sold_amount_raw,
  dexs.token_bought_address,
  dexs.token_sold_address,
  dexs.taker,
  dexs.maker,
  dexs.project_contract_address,
  dexs.tx_hash,
  dexs.evt_index
from dexs
