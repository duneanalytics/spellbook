{% set chain = 'solana' %}
{% set owners_observation_start_date = '2020-10-01' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_non_circulating_inventory_accounts',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    unique_key = ['token_mint_address', 'token_account']
  )
}}

-- non-circulating inventory token accounts for spl stablecoins on solana extended lineage
-- approach:
-- 1) reuse canonical non-circulating token accounts from the base helper table
-- 2) derive observed owners from extended stablecoin transfer history via from/to token-account matches
-- this keeps exclusions generic in runtime logic (no stale-age/threshold heuristics)
-- source: https://github.com/solana-labs/token-list/blob/main/src/tokens/solana.tokenlist.json
-- ref: https://www.circle.com/blog/gateway-new-pre-mint-address-for-usdc-on-solana

with token_accounts as (
  select
    token_mint_address,
    token_account,
    source_class
  from {{ ref('stablecoins_' ~ chain ~ '_non_circulating_inventory_accounts') }}
),

relevant_token_accounts as (
  select
    a.token_mint_address,
    a.token_account,
    a.source_class
  from token_accounts as a
  inner join {{ ref('tokens_' ~ chain ~ '_spl_stablecoins_extended') }} as s
    on s.token_mint_address = a.token_mint_address
),

owner_candidates as (
  select
    a.token_mint_address,
    a.token_account,
    t.from_owner as address
  from relevant_token_accounts as a
  inner join {{ ref('stablecoins_' ~ chain ~ '_extended_transfers') }} as t
    on t.token_mint_address = a.token_mint_address
    and a.token_account = t.from_token_account
  where t.block_date >= date '{{ owners_observation_start_date }}'

  union all

  select
    a.token_mint_address,
    a.token_account,
    t.to_owner as address
  from relevant_token_accounts as a
  inner join {{ ref('stablecoins_' ~ chain ~ '_extended_transfers') }} as t
    on t.token_mint_address = a.token_mint_address
    and a.token_account = t.to_token_account
  where t.block_date >= date '{{ owners_observation_start_date }}'
),

observed_owners as (
  select
    token_mint_address,
    token_account,
    array_join(array_sort(array_agg(distinct address)), ', ') as observed_owners
  from owner_candidates
  where address is not null
  group by 1, 2
)

select
  '{{ chain }}' as blockchain,
  a.token_mint_address,
  a.token_account,
  a.source_class,
  cast(a.source_class = 'legacy_inventory' as boolean) as excluded,
  o.observed_owners
from relevant_token_accounts as a
left join observed_owners as o
  on o.token_mint_address = a.token_mint_address
  and o.token_account = a.token_account
