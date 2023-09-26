{{
  config(
    tags = ['dunesql'],
    schema = 'mento_celo',
    alias = alias('trades_v2_dex')
  )
}}

--Mento v2
select
  t.evt_block_time as block_time,
  t.trader as taker,
  cast(null as varbinary) as maker,
  t.amountOut as token_bought_amount_raw,
  t.amountIn as token_sold_amount_raw,
  cast(null as double) as amount_usd,
  t.tokenOut as token_bought_address,
  t.tokenIn as token_sold_address,
  t.contract_address as project_contract_address,
  t.evt_tx_hash as tx_hash,
  t.evt_index
from {{ source('mento_celo', 'Broker_evt_Swap') }} t
{% if is_incremental() %}
where t.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
