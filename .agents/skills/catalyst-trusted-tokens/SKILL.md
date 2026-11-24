---
name: catalyst-trusted-tokens
description: add trusted tokens for a chain in prices_v2_trusted_tokens
disable-model-invocation: true
---

# catalyst trusted tokens

## overview
adds trusted tokens for a chain.

**when to use:** use when onboarding or updating trusted token coverage for a chain in prices v2 dex pricing inputs.

## parameters
- `<issue_id>`: linear issue id (e.g., CUR2-554)
- `<chain>`: chain name (e.g., xlayer, monad)

## usage
```
/catalyst-trusted-tokens CUR2-554 xlayer
```

dune mcp: for ad-hoc sql use `createAndExecuteQuery` (temporary by default, returns rows). to run a saved query (`query_id` given below), use `executeQueryById` then `getExecutionResults`.

## conventions
- **execution order:** numbered items are sequential. any prerequisite step is blocking and must complete before moving on.
- **code patterns:** use existing chain patterns as reference (e.g., `kaia`, `mezo`, `viction`). keep ordering consistent with existing files, or alphabetical when unclear.
- **chain substitutions:** replace chain name in file paths, model names, schema entries, and `blockchain` values consistently.
- **contributors:** new files should set git username only. existing files should append git username.
- **sql style:** when updating, mimic nearby style. for new sql, use lowercase keywords, trailing commas, single-space inline padding, no trailing semicolon, explicit `inner join` instead of `join`, positional `group by`, and one blank line between CTEs.

## prep vars
- retrieve chain metadata:
  - run via `createAndExecuteQuery`: `select * from dune.blockchains where name = '<chain>'`
- retrieve first block time:
  - run via `createAndExecuteQuery`: `select min(time) from <chain>.blocks where number <> 0`
- extract and keep handy:
  - `chain_id`
  - `name` (display name)
  - `token_address` (native token)

## git workflow
1. **verify main is up to date**
   - fetch latest, pull if behind, exit if diverged
2. **create branch**
   - branch name: `<issue_id>-<chain>-trusted-tokens-forward-only`
   - create from `main`, checkout, warn if already exists
   - do not commit/push as part of this workflow

## steps
1. **retrieve top token transfers and identify symbols**
  - check chain official docs/site for token addresses and symbols
  - ensure native token variable is either `0x0000..0000` or matches official docs
    - warn if this condition is not met
  - if details are missing:
    - use dune mcp **executeQueryById** with:
      - `query_id: 6293737`
      - `query_parameters: [{"key":"chain","value":"<chain>","type":"text"}]`
    - fetch result with **getExecutionResults** using returned `execution_id`
    - identify token symbols via chain explorer/scanner

2. **add trusted tokens**
  - edit `models/tokens/prices-v2/1_pipelines/1.2_dex/0_dex_prices/prices_v2_trusted_tokens.sql`
  - add to `manual_trusted_tokens` values (alphabetically):
    - `, ('<chain>', 0x<address>) -- <symbol>`
    - omit comment if symbol is unknown

## final checks
- fix any lint/compile issues introduced by the change set

