{{
  config(
    schema = 'polymarket_polygon',
    alias = 'users_capital_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time','evt_index','tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

safe_proxies as (
  select proxy from {{ ref('polymarket_polygon_users_safe_proxies') }}
  UNION ALL 
  select proxy from {{ ref('polymarket_polygon_users_magic_wallet_proxies') }}
),

polymarket_addresses as (
  select * from (values 
    (0x4D97DCd97eC945f40cF65F87097ACe5EA0476045), -- Conditional Tokens
    (0x3A3BD7bb9528E159577F7C2e685CC81A765002E2), -- Wrapped Collateral
    (0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E), -- CTFExchange
    (0xC5d563A36AE78145C45a50134d48A1215220f80a), -- NegRiskCTFExchange
    (0xc288480574783BD7615170660d71753378159c47),  -- Polymarket Rewards
    (0x94a3db2f861b01c027871b08399e1ccecfc847f6)   -- liq mining merkle distributor
  ) as t(address)
  UNION ALL 
  select 
    address
  from {{ source('polygon', 'creation_traces') }}
  where "from" = 0x8b9805a2f595b6705e74f7310829f2d299d21522
  -- these are fpmm contracts
)

select
  block_time,
  block_date,
  block_number,
  'deposit' as action,
  "from" as from_address,
  "to" as to_address,
  symbol,
  amount_raw,
  amount,
  amount_usd,
  evt_index,
  tx_hash
from {{ source('tokens_polygon', 'transfers')}}
where contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC
  and "to" in (select proxy from safe_proxies)
  and "from" not in (select proxy from safe_proxies)
  and "to" not in (select address from polymarket_addresses)
  and "from" not in (select address from polymarket_addresses)
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}

union all

select
  block_time,
  block_date,
  block_number,
  'withdrawal' as action,
  "from" as from_address,
  "to" as to_address,
  symbol,
  amount_raw,
  amount,
  amount_usd,
  evt_index,
  tx_hash
from {{ source('tokens_polygon', 'transfers')}}
where contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC
  and "from" in (select proxy from safe_proxies)
  and "to" not in (select proxy from safe_proxies)
  and "to" not in (select address from polymarket_addresses)
  and "from" not in (select address from polymarket_addresses)
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}

union all

select distinct
  block_time,
  block_date,
  block_number,
  'transfer' as action, -- transfer between safes
  "from" as from_address,
  "to" as to_address,
  symbol,
  amount_raw,
  amount,
  amount_usd,
  evt_index,
  tx_hash
from {{ source('tokens_polygon', 'transfers')}}
where contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC.e
  and "from" in (select proxy from safe_proxies)
  and "to" in (select proxy from safe_proxies)
  and "to" not in (select address from polymarket_addresses)
  and "from" not in (select address from polymarket_addresses)
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}
