{% set chain = 'solana' %}

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
-- 2) expand the seed set with every active token account currently owned by an
--    address in `stablecoins_solana_non_circulating_inventory_owners`, resolved via
--    the indexer-maintained `solana_utils_token_accounts` mapping. This avoids
--    walking transfer history and also catches accounts that received the
--    stablecoin via mint events (not just SPL Transfer instructions).
-- 3) annotate each account with its current owner (for traceability)
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

excluded_owners as (
  select token_mint_address, owner_address
  from {{ ref('stablecoins_' ~ chain ~ '_non_circulating_inventory_owners') }}
  where excluded
),

-- token accounts derived from the curated owners list via the indexer state map
owner_derived_accounts as (
  select
    ta.token_mint_address,
    ta.address as token_account
  from {{ ref('solana_utils_token_accounts') }} as ta
  inner join excluded_owners as o
    on o.token_mint_address = ta.token_mint_address
    and o.owner_address = ta.token_balance_owner
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
)

select
  '{{ chain }}' as blockchain,
  c.token_mint_address,
  c.token_account,
  c.source_class,
  true as excluded,
  ta.token_balance_owner as observed_owners
from classified_accounts as c
left join {{ ref('solana_utils_token_accounts') }} as ta
  on ta.address = c.token_account
  and ta.token_mint_address = c.token_mint_address
