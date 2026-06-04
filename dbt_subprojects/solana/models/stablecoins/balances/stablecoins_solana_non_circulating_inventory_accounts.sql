{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'non_circulating_inventory_accounts',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['token_mint_address', 'token_account']
  )
}}

-- non-circulating inventory token accounts for spl stablecoins on solana (core + extended)
-- approach:
-- 1) seed known non-circulating token accounts inline via values() (seeded_accounts)
-- 2) seed known non-circulating owner wallets inline via values() (seeded_owners) and
--    resolve them to their token accounts via the indexer state-history table. This
--    covers owners (e.g. Circle treasury) that hold balances across many token
--    accounts, and catches accounts that received the stablecoin via mint events
--    (which SPL Transfer history misses).
-- 3) merge: seeded token accounts win on classification when an account also surfaces
--    via owner derivation, so the richer source_class is preserved
--
-- incremental: owner-derived accounts are appended as the indexer observes new token
-- accounts for the seeded owners, watermarked on state_history.valid_from_block_time.
-- exclusions are permanent (a closed/transferred account left in the list is harmless:
-- the balances macro only ever LEFT JOINs against it). The seed lists are written on
-- full-refresh only, so SEED OR SCOPE EDITS REQUIRE `dbt run --full-refresh`. A periodic
-- full-refresh also reconciles edge cases (owner changes, closures).
-- source: https://github.com/solana-labs/token-list/blob/main/src/tokens/solana.tokenlist.json
-- ref: https://www.circle.com/blog/gateway-new-pre-mint-address-for-usdc-on-solana

with
-- mints in scope for this exclusion list (currently USDC only — extend here
-- when other stablecoins need owner/account-level exclusions)
in_scope_mints as (
  select token_mint_address
  from (values
    ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')  -- usdc
  ) as t(token_mint_address)
),

-- (2) hand-curated non-circulating owner wallets
seeded_owner_rows as (
  select token_mint_address, owner_address, source_class
  from (
    values
      -- usdc: Circle Mint authority (manages pre-mint operations, issuer_operations in curated-stablecoins labels)
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE', 'circle_mint'),
      -- usdc: Circle Treasury (issuer_treasury in curated-stablecoins labels)
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '41zCUJsKk6cMB94DDtm99qWmyMZfp4GkAhhuz4xTwePu', 'circle_treasury'),
      -- usdc: Wormhole bridge to FOGO chain (locked liquidity, not circulating on Solana)
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '42qwJUTbKf3D8ULfWadUSjnHf6pkJ4H1VjCcfSKHvDTN', 'wormhole_bridge')
  ) as t(token_mint_address, owner_address, source_class)
),

seeded_owners as (
  select s.token_mint_address, s.owner_address, s.source_class
  from seeded_owner_rows as s
  inner join in_scope_mints as m
    on m.token_mint_address = s.token_mint_address
),

-- resolve seeded owners to their token accounts via indexer state-history.
-- read state_history directly (not the solana_utils view) so the owner filter
-- bounds the scan and we get valid_from_block_time for the incremental watermark.
owner_derived_accounts as (
  select
    sh.token_mint_address,
    sh.address as token_account,
    o.source_class,
    sh.token_balance_owner as observed_owners,
    sh.valid_from_block_time as indexed_at
  from {{ source('token_accounts_solana', 'state_history') }} as sh
  inner join seeded_owners as o
    on o.token_mint_address = sh.token_mint_address
    and o.owner_address = sh.token_balance_owner
  where sh.is_active = true
  {% if is_incremental() %}
    and sh.valid_from_block_time >= (select coalesce(max(indexed_at), timestamp '1970-01-01 00:00:00 UTC') from {{ this }})
  {% endif %}
)

{% if not is_incremental() %}
,
-- (1) hand-curated non-circulating token accounts — seeded on full-refresh only
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

seeded_accounts as (
  select
    s.token_mint_address,
    s.token_account,
    s.source_class,
    cast(null as varchar) as observed_owners,
    cast(null as timestamp(3) with time zone) as indexed_at
  from seeded_account_rows as s
  inner join in_scope_mints as m
    on m.token_mint_address = s.token_mint_address
),

all_candidates as (
  select token_mint_address, token_account, source_class, 1 as priority, observed_owners, indexed_at from seeded_accounts
  union all
  select token_mint_address, token_account, source_class, 2 as priority, observed_owners, indexed_at from owner_derived_accounts
)
{% else %}
,
all_candidates as (
  select token_mint_address, token_account, source_class, 2 as priority, observed_owners, indexed_at from owner_derived_accounts
)
{% endif %}

-- merge with priority: seeded token accounts (priority 1) win over owner-derived
-- (priority 2) so the more specific source_class survives the dedupe
select
  '{{ chain }}' as blockchain,
  token_mint_address,
  token_account,
  min_by(source_class, priority) as source_class,
  true as excluded,
  max(observed_owners) as observed_owners,
  max(indexed_at) as indexed_at
from all_candidates
group by 1, 2, 3
