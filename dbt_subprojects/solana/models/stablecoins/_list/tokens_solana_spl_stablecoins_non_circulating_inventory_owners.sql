{% set chain = 'solana' %}
{% set owners_observation_start_date = '2020-10-01' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'spl_stablecoins_non_circulating_inventory_owners',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    unique_key = ['token_mint_address', 'token_account']
  )
}}

-- non-circulating inventory token accounts for spl stablecoins on solana
-- approach:
-- 1) curate known non-circulating token accounts inline via values() (not dbt seed-backed)
-- 2) derive their owners from transfer history
-- this keeps exclusions generic in runtime logic (no stale-age/threshold heuristics)
-- source: https://github.com/solana-labs/token-list/blob/main/src/tokens/solana.tokenlist.json
-- ref: https://www.circle.com/blog/gateway-new-pre-mint-address-for-usdc-on-solana

with token_accounts as (
  select token_mint_address, token_account, source
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
  ) as t(token_mint_address, token_account, source)
),

token_accounts_agg as (
  select
    token_mint_address,
    token_account,
    array_join(array_sort(array_agg(distinct source)), ', ') as sources
  from token_accounts
  group by 1, 2
),

owner_candidates as (
  select
    a.token_mint_address,
    a.token_account,
    case
      when a.token_account = t.from_token_account then t.from_owner
      when a.token_account = t.to_token_account then t.to_owner
    end as address
  from token_accounts_agg as a
  inner join {{ source('tokens_' ~ chain, 'transfers') }} as t
    on t.token_mint_address = a.token_mint_address
    and (a.token_account = t.from_token_account or a.token_account = t.to_token_account)
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
  o.observed_owners,
  a.sources
from token_accounts_agg as a
left join observed_owners as o
  on o.token_mint_address = a.token_mint_address
  and o.token_account = a.token_account
