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

-- non-circulating inventory token accounts for spl stablecoins on solana
-- approach:
-- 1) curate known non-circulating token accounts inline via values() (not dbt seed-backed)
-- 2) derive observed owners from stablecoin transfer history via from/to token-account matches
-- this keeps exclusions generic in runtime logic (no stale-age/threshold heuristics)
-- source: https://github.com/solana-labs/token-list/blob/main/src/tokens/solana.tokenlist.json
-- ref: https://www.circle.com/blog/gateway-new-pre-mint-address-for-usdc-on-solana

with token_accounts as (
  select token_mint_address, token_account, source_class
  from (
    values
      -- usdc mint
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '27T5c11dNMXjcRuko9CeUy3Wq41nFdH3tz9Qt4REzZMM', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '28VqfqsUUBx59i8ruG2TuC5RekW5ZY3tsK4bSV59sXjn', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'FSxJ85FXVsXSr51SeWf9ciJWTcRnqKFSmBgRDeL3KyWw', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '6xTBTqJMBr5m7BKqVxmW2x11DfqUwtD3TJsqpxELx72L', 'official_circle_premint'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'CkzX3bvAt9PcjCh2QoQdM9ENzUVwH229hFe4dB7Y8qZK', 'official_circle_premint'),
      -- legacy usdc non-circulating inventory token accounts
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'fMx1JWj55yTMv4CFLm5ZRWjo16TnbsQDsVuCbkBDnYe', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '5nGMuvwbZdZZtXQdGhCnGfu6oCDmikXi3yChmdb4GBrV', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '5tFhdTCzTYMvfVTZnczZEL36YjFnkDTSaoQ7XAZvS7LR', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'FYAFDcQsZCgJJdj5YJNLPsWazYyqDTWmgyX7hFk4mM95', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'fBQG6bx8SFgAVcS3vtr3rJDFKHnVcKw4CpbL3o7obBu', 'legacy_inventory'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '9tKZxccYcSGDYTTgkme8mG2XbZpaYJaKK6DmrT8ZHy9R', 'legacy_inventory')
  ) as t(token_mint_address, token_account, source_class)
),

owner_candidates as (
  select
    a.token_mint_address,
    a.token_account,
    t.from_owner as address
  from token_accounts as a
  inner join {{ ref('stablecoins_' ~ chain ~ '_transfers') }} as t
    on t.token_mint_address = a.token_mint_address
    and a.token_account = t.from_token_account
  where t.block_date >= date '{{ owners_observation_start_date }}'

  union all

  select
    a.token_mint_address,
    a.token_account,
    t.to_owner as address
  from token_accounts as a
  inner join {{ ref('stablecoins_' ~ chain ~ '_transfers') }} as t
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
from token_accounts as a
left join observed_owners as o
  on o.token_mint_address = a.token_mint_address
  and o.token_account = a.token_account
