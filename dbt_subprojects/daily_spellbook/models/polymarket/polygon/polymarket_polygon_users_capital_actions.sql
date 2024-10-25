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
                                  contributors = \'["tomfutago, 0xBoxer"]\') }}'
  )
}}

-- lots of edge cases here to ensure that we're just picking up on actual deposits and not internal transfers
-- this is a bit of a mess, but it works for now

-- we look for usdc.e and usdc transfers
-- usdc.e is the wrapped version of usdc on polygon polymarket runs on this
-- if you deposit using usdc, the UI will prompt you to wrap your USDC into USDC.e by signing a message
-- this will just use uniswap to swap your usdc for usdc.e, so we need to exclude 0xD36ec33c8bed5a9F7B6630855f1533455b98a418 as this is the uniswap pool
-- by ignoring the uniswap pool, but looking for USDC transfers, we can get a better read on funding sources


-- get all safe and magic wallet proxies to filter for polymarket user addresses
-- there are some rare EOA addresses that trade directly on polymarket, but they are few and far between
with safe_proxies as (
  select proxy from {{ ref('polymarket_polygon_users_safe_proxies') }}
  UNION ALL 
  select proxy from {{ ref('polymarket_polygon_users_magic_wallet_proxies') }}
),

-- get all known polymarket contract and filter them out as these are not users
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

-- get all deposits

select
  block_time,
  block_date,
  block_number,
  'deposit' as action,
  "from" as from_address,
  "to" as to_address,
  case when contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 then 'USDC.e'
    when contract_address = 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359 then 'USDC'
  end as symbol,
  amount_raw,
  amount,
  amount_usd,
  evt_index,
  tx_hash
from {{ source('tokens_polygon', 'transfers')}}
where (contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC.e
  or contract_address = 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359) -- USDC
  and "to" in (select proxy from safe_proxies)
  and "from" not in (select proxy from safe_proxies)
  and "to" not in (select address from polymarket_addresses)
  and "from" not in (select address from polymarket_addresses)
  and "from" <> 0xD36ec33c8bed5a9F7B6630855f1533455b98a418 --USDC.e - USDC uniswap pool
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}

union all

-- get all withdrawals

select
  block_time,
  block_date,
  block_number,
  'withdrawal' as action,
  "from" as from_address,
  "to" as to_address,
  case when contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 then 'USDC.e'
    when contract_address = 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359 then 'USDC'
  end as symbol,
  amount_raw,
  amount,
  amount_usd,
  evt_index,
  tx_hash
from {{ source('tokens_polygon', 'transfers')}}
where (contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC.e
  or contract_address = 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359) -- USDC
  and "from" in (select proxy from safe_proxies)
  and "to" not in (select proxy from safe_proxies)
  and "to" not in (select address from polymarket_addresses)
  and "from" not in (select address from polymarket_addresses)
  and "from" <> 0xD36ec33c8bed5a9F7B6630855f1533455b98a418 --USDC.e - USDC uniswap pool
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}

union all

-- get all transfers between safes, this is very rare but a possible edge case

select distinct
  block_time,
  block_date,
  block_number,
  'transfer' as action, -- transfer between safes
  "from" as from_address,
  "to" as to_address,
  case when contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 then 'USDC.e'
    when contract_address = 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359 then 'USDC'
  end as symbol,
  amount_raw,
  amount,
  amount_usd,
  evt_index,
  tx_hash
from {{ source('tokens_polygon', 'transfers')}}
where (contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC.e
  or contract_address = 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359) -- USDC
  and "from" in (select proxy from safe_proxies)
  and "to" in (select proxy from safe_proxies)
  and "to" not in (select address from polymarket_addresses)
  and "from" not in (select address from polymarket_addresses)
  and "from" <> 0xD36ec33c8bed5a9F7B6630855f1533455b98a418 --USDC.e - USDC uniswap pool
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}
