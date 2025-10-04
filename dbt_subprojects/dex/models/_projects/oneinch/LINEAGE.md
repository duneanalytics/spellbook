# 1inch dbt Lineage Overview

This note summarizes how the 1inch project inside `dbt_subprojects/dex` is wired together: where each table pulls data from, what key transforms happen, and how the downstream outputs fit into dashboards such as `oneinch_swaps` and the DEX trade marts.

## Meta configuration (single source of truth)
- `macros/models/_project/oneinch/_meta/oneinch_meta_cfg_macro.sql` centralizes blockchain coverage, launch/start dates, wrapped native tokens, settlement & escrow contract lists, and per-stream contract/method catalogs.
- All models/macros look up this meta map to stay chain-agnostic; adding a new chain or contract largely means updating the config macros (`oneinch_ar_cfg_contracts_macro`, `oneinch_lo_cfg_contracts_macro`, `oneinch_cc_cfg_contracts_macro`).

## Raw call extraction layer
- For each EVM chain the project cares about (e.g. `models/_projects/oneinch/arbitrum/…`):
  - `oneinch_<chain>_<stream>_raw_calls.sql` invokes `oneinch_raw_calls_macro`. That macro scans decoded call sources such as `source('oneinch_<chain>', '<contract>_call_<method>')` and filters on the canonical transaction `traces` table to capture every execution of the 1inch contract selectors since the configured start date.
  - Results are stored incrementally (Delta format) with partitioning by `block_month` and keyed by `tx_hash` + `call_trace_address`.
- `models/_projects/oneinch/oneinch_evms_raw_calls.sql` simply UNION ALLs those per-chain tables into a single cross-chain view for convenience.

## Parsed transfer layer
- `macros/models/_project/oneinch/oneinch_ptfc_macro.sql` is the workhorse that replays trace input payloads to recognize ERC-20 `transfer`, `transferFrom`, `mint`, `burn`, wrapper `deposit/withdraw`, and native value movements. It emits normalized transfer rows with sender/receiver, amount, token id, and transfer trace path.
- `oneinch_raw_transfers_macro` (per chain) joins:
  1. All successful call records pulled **directly** from the raw-call tables (`oneinch_<chain>_<stream>_raw_calls`) to anchor which transactions are 1inch-related. Stream models such as `oneinch_<chain>_ar` are *not* a dependency here — both the call streams and the transfer layer read from the same raw call staging.
  2. Parsed transfers from `oneinch_ptfc_macro` limited to matching transactions.
  3. Token metadata (`source('tokens','erc20')`), trusted-token flags, and `prices.usd` feed to compute human-readable symbols/decimals and USD notional.
- Per-chain models such as `models/_projects/oneinch/arbitrum/oneinch_arbitrum_raw_transfers.sql` materialize those enriched transfers; `models/_projects/oneinch/oneinch_evms_raw_transfers.sql` unions them for cross-chain analysis.

## Stream-specific call models
Each stream macro enriches raw calls with decoded parameters, derived flags, and pricing:

- **Aggregation Router (`AR`)** – `macros/models/_project/oneinch/AR/oneinch_ar_macro.sql` combines decoded call arguments (pool paths, token amounts, receivers) with raw call telemetry. It corrects auxiliary invocations, determines pool token addresses, and attaches native prices. Chain modules like `oneinch_arbitrum_ar.sql` run this macro.

- **Limit Order Protocol & Fusion (`LO`)** – `macros/models/_project/oneinch/LOP/oneinch_lo_macro.sql` parses limit order payloads, recognises whether settlements/factories are involved, and sets boolean flags (`partial`, `multiple`, `fusion`, `cross_chain`, etc.). It shares logic with the cross-chain stream and powers `oneinch_<chain>_lo.sql` tables.

- **Cross-Chain (`CC`)** – `macros/models/_project/oneinch/CC/oneinch_cc_macro.sql` stitches together three sub-macros: source escrow creations, destination escrow creations, and completion/result handlers. It relies on the LO macro to reuse decoding logic and assembles hashlock-based flows.

The outputs are per-chain incremental tables keyed by call (e.g. `oneinch_arbitrum_ar`, `oneinch_arbitrum_lo`, `oneinch_arbitrum_cc`). Consolidated views (`oneinch_evms_ar.sql`, `oneinch_evms_lo.sql`, `models/_projects/oneinch/oneinch_cc.sql`) provide a unified surface for downstream models.

## Execution assembly
Execution-layer macros correlate call-level intents with the actual tokens that moved to quantify trades and fees:

- `oneinch_ar_executions_macro`, `oneinch_lo_executions_macro`, and `oneinch_cc_executions_macro` join call tables with the parsed transfers. They compute USD notionals, infer what the user actually sent/received, mark trusted pricing, derive per-call execution cost (gas * native price), and carry complementary metadata (e.g. dst chain ids, hashlocks, trait bits).
- Per-chain incremental tables (`oneinch_<chain>_<stream>_executions.sql`) materialize these enriched execution rows.
- `models/_projects/oneinch/oneinch_cc_executions.sql` additionally merges cross-chain iterations across source/destination chains and aggregates actions such as escrow creation, withdrawal, and settlement, using the unioned EVMS transfer view.

## Unified execution & swaps marts
- `models/_projects/oneinch/oneinch_executions.sql` UNION ALLs every execution flavour (AR classic, LO limits & fusion, CC, and a Solana placeholder) into one incremental fact table. It normalizes the field list, tags mode (`classic`, `limits`, `fusion`, `cross-chain`), mirrors complementary data into `flags`, and generates a deterministic `id` surrogate key.
- `models/_projects/oneinch/oneinch_swaps.sql` groups executions by `(blockchain, dst_blockchain, order_hash, user)` to produce a swap-level rollup with aggregated USD volume, execution cost, aggregated remains/flags, and an array of child execution descriptors. This is the main consumption table for product/BI teams wanting “what happened to an order across chains”.
- `models/_projects/oneinch/oneinch_evms_aggregation_trades.sql` and `oneinch_evms_limit_trades.sql` reshape executions into the canonical DEX trade schema used by Spellbook’s shared `dex.trades` mart (token bought/sold, taker/maker, amount_usd, etc.).

## Supporting metadata & token models
- `_meta` models (`oneinch_blockchains.sql`, `oneinch_fusion_resolvers.sql`, `oneinch_fusion_executors.sql`, `oneinch_fusion_farms.sql`, `oneinch_fusion_accounts.sql`) provide descriptive tables for settlement infrastructure, resolver KYC status, executor privileges, farm configuration, and resolver EOAs. They mostly draw from Ethereum logs, the whitelist registries, and project-specific sources, with incremental change tracking where needed.
- `_tokens` models:
  - `oneinch_aave_tokens.sql` snapshots the latest aToken deployments across supported chains from the Aave v3 events.
  - `oneinch_ondo_tokens.sql` tracks Ondo GM tokens on Ethereum.
- These tables support enrichment and sanity checks elsewhere in the ecosystem.

## Key data sources referenced throughout
- Core EVM primitives: `source(<chain>, 'traces')`, `'transactions'`, and `'logs'`.
- Decoded contract ABIs: `source('oneinch_<chain>', '<contract>_call_<method>')` and `'<contract>_evt_<event>'`.
- Market data: `source('prices', 'usd')` and `source('prices', 'trusted_tokens')`.
- Token metadata: `source('tokens', 'erc20')`.
- Specialized feeds: e.g. `source('oneinch_solana', 'swaps')`, `source('aave_v3_<chain>', 'AToken_evt_Initialized')`, `source('ondo_finance_ethereum', 'GMTokenFactory_evt_NewGMTokenDeployed')`.

## Putting it together
1. **Meta config** declares what to watch.
2. **Raw call layer** captures every 1inch contract call per stream & chain.
3. **Parsed transfers** run in parallel off the same raw-call staging to catalogue the token movements for those transactions.
4. **Stream models** decode the raw calls into stream-specific facts with arguments, flags, and pricing context.
5. **Execution macros** join the decoded call facts with the transfer layer to align intent with realised token flow and pricing.
6. **Unified marts** aggregate per-order/per-trade outputs for analytics, while metadata tables maintain lookup information for infrastructure participants.

Maintaining lineage generally means updating the meta config/macros first, then letting the existing per-chain pattern (raw calls feeding both stream models and transfer staging, which execution models merge) populate the shared EVMS views and marts.
