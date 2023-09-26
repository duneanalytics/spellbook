{{
    config(
        tags = ['dunesql'],
        schema = 'mento_celo',
        alias = alias('trades_v1_dex')
    )
}}

--Mento v1
select
  t.evt_block_time as block_time,
  t.exchanger as taker,
  cast(null as varbinary) as maker,
  t.buyAmount as token_bought_amount_raw,
  t.sellAmount as token_sold_amount_raw,
  cast(null as double) as amount_usd,
  case
    when t.soldGold then 0x765DE816845861e75A25fCA122bb6898B8B1282a -- cUSD
    else 0x471EcE3750Da237f93B8E339c536989b8978a438                 -- CELO
  end as token_bought_address,
  case
    when t.soldGold then 0x471EcE3750Da237f93B8E339c536989b8978a438 -- CELO
    else 0x765DE816845861e75A25fCA122bb6898B8B1282a                 -- cUSD
  end as token_sold_address,
  t.contract_address as project_contract_address,
  t.evt_tx_hash as tx_hash,
  t.evt_index
from {{ source('mento_celo', 'Exchange_evt_Exchanged') }} t
{% if is_incremental() %}
where t.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

-- TODO: add 2 other pairs
