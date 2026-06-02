{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'non_circulating_inventory_accounts',
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['token_mint_address', 'token_account']
  )
}}

-- non-circulating inventory token accounts for spl stablecoins on solana (core + extended)
-- approach:
-- 1) seed known non-circulating token accounts inline via values() (seeded_accounts)
-- 2) seed known non-circulating owner wallets inline via values() (seeded_owners) and
--    resolve them to token accounts at build time via the indexer-maintained
--    `solana_utils_token_accounts` mapping. This covers owners (e.g. Circle treasury)
--    that hold balances across many token accounts, and catches accounts that received
--    the stablecoin via mint events (which SPL Transfer history misses).
-- 3) merge: seeded token accounts win on classification when an account also surfaces
--    via owner derivation, so the richer source_class is preserved
-- 4) annotate each account with its current owner from the state map (for traceability)
-- source: https://github.com/solana-labs/token-list/blob/main/src/tokens/solana.tokenlist.json
-- ref: https://www.circle.com/blog/gateway-new-pre-mint-address-for-usdc-on-solana
-- ref: https://github.com/DefiLlama/peggedassets-server/blob/master/src/adapters/peggedAssets/usd-coin/config.ts

with
-- mints in scope for this exclusion list (currently USDC only — extend here
-- when other stablecoins need owner/account-level exclusions)
in_scope_mints as (
  select token_mint_address
  from (values
    ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')  -- usdc
  ) as t(token_mint_address)
),

-- (1) hand-curated non-circulating token accounts
seeded_account_rows as (
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

-- (2) hand-curated non-circulating owner wallets
seeded_owner_rows as (
  select token_mint_address, owner_address, source_class
  from (
    values
      -- usdc: circle treasury / mint wallets (also tracked by DefiLlama as non-circulating)
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE', 'circle_treasury'),
      -- usdc: additional non-circulating owners tracked by DefiLlama
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '41zCUJsKk6cMB94DDtm99qWmyMZfp4GkAhhuz4xTwePu', 'defillama_excluded'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '42qwJUTbKf3D8ULfWadUSjnHf6pkJ4H1VjCcfSKHvDTN', 'defillama_excluded')
  ) as t(token_mint_address, owner_address, source_class)
),

-- defensive filters: only emit rows for in-scope mints
seeded_accounts as (
  select s.token_mint_address, s.token_account, s.source_class
  from seeded_account_rows as s
  inner join in_scope_mints as m
    on m.token_mint_address = s.token_mint_address
),

seeded_owners as (
  select s.token_mint_address, s.owner_address, s.source_class
  from seeded_owner_rows as s
  inner join in_scope_mints as m
    on m.token_mint_address = s.token_mint_address
),

-- (2 cont.) resolve owner wallets to their token accounts via indexer state
owner_derived_accounts as (
  select
    o.token_mint_address,
    ta.address as token_account,
    o.source_class
  from {{ ref('solana_utils_token_accounts') }} as ta
  inner join seeded_owners as o
    on o.token_mint_address = ta.token_mint_address
    and o.owner_address = ta.token_balance_owner
),

-- (3) merge with priority: seeded token accounts (priority 1) win over owner-derived
-- (priority 2) so the more specific source_class survives the dedupe
classified_accounts as (
  select
    token_mint_address,
    token_account,
    min_by(source_class, priority) as source_class
  from (
    select token_mint_address, token_account, source_class, 1 as priority from seeded_accounts
    union all
    select token_mint_address, token_account, source_class, 2 as priority from owner_derived_accounts
  )
  group by 1, 2
)

-- (4) annotate with current owner from indexer state
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
