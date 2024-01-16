{{
  config(
    schema = 'carbonhood_celo',
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
  'carbonhood' as project,
  '1' as version,
  cast(date_trunc('month', t.evt_block_time) as date) as block_month,
  cast(t.evt_block_time as date) as block_date,
  t.evt_block_time as block_time,
  t.evt_block_number as block_number,
  t.account as taker,
  cast(null as varbinary) as maker,
  t.amountOut as token_bought_amount_raw,
  t.amountIn as token_sold_amount_raw,
  t.tokenOut as token_bought_address,
  t.tokenIn as token_sold_address,
  t.contract_address as project_contract_address,
  t.evt_tx_hash as tx_hash,
  t.evt_index
from {{ source('carbonhood_celo', 'Router_evt_Swap') }} t
{% if is_incremental() %}
where {{ incremental_predicate('t.evt_block_time') }}
{% endif %}
