{% set chain = 'solana' %}
{% set owners_observation_start_date = '2020-10-01' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'non_circulating_inventory_accounts',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    unique_key = ['token_mint_address', 'token_account']
  )
}}

-- non-circulating inventory token accounts for spl stablecoins on solana (core + extended)
-- approach:
-- 1) seed known non-circulating token accounts inline via values() (not dbt seed-backed)
-- 2) expand the seed set with token accounts derived from the curated owners list
--    (`stablecoins_solana_non_circulating_inventory_owners`) by walking core+extended
--    transfer history. resolving owners → token accounts at build time keeps the
--    runtime exclusion shape to a single token-account-keyed left join in the
--    balances macro.
-- 3) annotate each account with observed owners from transfer history (for traceability)
-- source: https://github.com/solana-labs/token-list/blob/main/src/tokens/solana.tokenlist.json
-- ref: https://www.circle.com/blog/gateway-new-pre-mint-address-for-usdc-on-solana

with seeded_accounts as (
  select token_mint_address, token_account, source_class
  from (
    values
      -- usdc official Circle premint accounts (non-circulating by Circle's own definition)
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '27T5c11dNMXjcRuko9CeUy3Wq41nFdH3tz9Qt4REzZMM', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '28VqfqsUUBx59i8ruG2TuC5RekW5ZY3tsK4bSV59sXjn', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'FSxJ85FXVsXSr51SeWf9ciJWTcRnqKFSmBgRDeL3KyWw', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '6xTBTqJMBr5m7BKqVxmW2x11DfqUwtD3TJsqpxELx72L', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'CkzX3bvAt9PcjCh2QoQdM9ENzUVwH229hFe4dB7Y8qZK', 'official_circle_premint'),
      -- usdc legacy non-circulating inventory token accounts
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'fMx1JWj55yTMv4CFLm5ZRWjo16TnbsQDsVuCbkBDnYe', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '5nGMuvwbZdZZtXQdGhCnGfu6oCDmikXi3yChmdb4GBrV', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '5tFhdTCzTYMvfVTZnczZEL36YjFnkDTSaoQ7XAZvS7LR', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'FYAFDcQsZCgJJdj5YJNLPsWazYyqDTWmgyX7hFk4mM95', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'fBQG6bx8SFgAVcS3vtr3rJDFKHnVcKw4CpbL3o7obBu', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '9tKZxccYcSGDYTTgkme8mG2XbZpaYJaKK6DmrT8ZHy9R', 'legacy_inventory')
  ) as t(token_mint_address, token_account, source_class)
),

all_transfers as (
  select block_date, token_mint_address, from_owner, from_token_account, to_owner, to_token_account
  from {{ ref('stablecoins_' ~ chain ~ '_core_transfers') }}
  union all
  select block_date, token_mint_address, from_owner, from_token_account, to_owner, to_token_account
  from {{ ref('stablecoins_' ~ chain ~ '_extended_transfers') }}
),

excluded_owners as (
  select token_mint_address, owner_address
  from {{ ref('stablecoins_' ~ chain ~ '_non_circulating_inventory_owners') }}
  where excluded
),

-- token accounts derived from the curated owners list: any token account that has
-- ever sent or received the stablecoin for an excluded owner is itself inventory.
owner_derived_accounts as (
  select distinct
    o.token_mint_address,
    t.from_token_account as token_account
  from excluded_owners as o
  inner join all_transfers as t
    on t.token_mint_address = o.token_mint_address
    and t.from_owner = o.owner_address
  where t.from_token_account is not null
    and t.block_date >= date '{{ owners_observation_start_date }}'

  union

  select distinct
    o.token_mint_address,
    t.to_token_account as token_account
  from excluded_owners as o
  inner join all_transfers as t
    on t.token_mint_address = o.token_mint_address
    and t.to_owner = o.owner_address
  where t.to_token_account is not null
    and t.block_date >= date '{{ owners_observation_start_date }}'
),

-- merge: seeded rows keep their richer source_class; owner-derived rows get
-- 'owner_derived'. priority dedups (mint, account) so a seeded account that also
-- shows up via owner-derivation stays classified as seeded.
classified_accounts as (
  select
    token_mint_address,
    token_account,
    min_by(source_class, priority) as source_class
  from (
    select token_mint_address, token_account, source_class, 1 as priority from seeded_accounts
    union all
    select token_mint_address, token_account, 'owner_derived' as source_class, 2 as priority from owner_derived_accounts
  )
  group by 1, 2
),

owner_candidates as (
  select
    a.token_mint_address,
    a.token_account,
    t.from_owner as address
  from classified_accounts as a
  inner join all_transfers as t
    on t.token_mint_address = a.token_mint_address
    and a.token_account = t.from_token_account
  where t.block_date >= date '{{ owners_observation_start_date }}'

  union all

  select
    a.token_mint_address,
    a.token_account,
    t.to_owner as address
  from classified_accounts as a
  inner join all_transfers as t
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
  c.token_mint_address,
  c.token_account,
  c.source_class,
  true as excluded,
  o.observed_owners
from classified_accounts as c
left join observed_owners as o
  on o.token_mint_address = c.token_mint_address
  and o.token_account = c.token_account
