# Aero bridge flows

This integration attributes **USDC bridge legs** to the deployed Aero Metarouter and feeds the existing `bridges_evms.flows` lineage. It does not report final Metaswap output tokens or recipients. The bridge recipient can be a Hyperlane interchain account. `sender` is the Metarouter that calls the bridge; `tx_from` is the transaction initiator.

## Contracts and coverage

[Aero's security documentation](https://app.aero.xyz/docs/security/) identifies the Metarouter at `0x0fEf9e88A7b76BCDBF451780C6774FE32965Ee98`. Its verified [CrosschainLib](https://github.com/dromos-labs/metadex-public/blob/8b506b450aaa9d4a9f1a77e264f2d9dd508e9ddf/V3/src/metarouter/libraries/CrosschainLib.sol) passes the bridge principal after token fees to `transferRemote`. Bridge and destination-plan messages are separate operations.

The route and local token addresses come from the [Hyperlane USDC registry](https://github.com/hyperlane-xyz/hyperlane-registry/blob/d350acb9a6ce045758be6fa610bb9e697b7fd1bc/deployments/warp_routes/USDC/eclipsemainnet-config.yaml). These routes are shared with other applications. All six routes have six token decimals and scale 1:1. Mailboxes come from the same registry's `chains/<chain>/addresses.yaml`. Hyperlane domains equal EVM chain IDs for this allowlist.

| Chain | USDC collateral router | Dune decoding check, 2026-11-02 |
| --- | --- | --- |
| Arbitrum | `0xad4350ee0f9f5b85bab115425426086ae8384ebb` | No decoded tables returned by address search |
| Arc | `0x56a255a080584de4433d0fe6587f4c0105715589` | No decoded tables returned by address search |
| Base | `0x37e637891a558b5b621723cbf8fc771525f280c1` | No decoded tables returned by address search |
| Ethereum | `0xe1de9910fe71cc216490ac7fcf019e13a34481d7` | No decoded tables returned by address search |
| Ink | `0x0de665377a5b0d390469d0ea0fcae55e0c14f4c4` | No decoded tables returned by address search |
| Optimism | `0x02bfd67829317d666dc7dfa030f18eacc12c2cfb` | No decoded tables returned by address search |

The Metarouter already has `aero_<chain>.metarouter_call_execute` on all six chains. `aero_multichain.metarouter_call_execute` also covers those chains and Robinhood; its discriminator is named `chain`. Live rows were checked through that namespace. The model reads only the per-chain tables to avoid duplicate counting across namespaces. The root and leaf message orchestrators and Hyperlane adapters are also decoded, but carry governance and reward-redemption messages, not the Metaswap USDC bridge leg.

**Decoding is incomplete.** The model uses existing decoded Metarouter calls to select eligible transactions, then canonical traces/logs for the Warp transfer and mailbox events. It does not reference nonexistent decoded route tables or require new decoding submissions. Future decoding should extend Hyperlane's existing exact contract namespace after checking both per-chain and multichain coverage.

## Data contract

- One deposit per persisted `DispatchId`, keyed by origin chain, source transaction hash and log index. The globally unique Hyperlane message ID matches both sides in `bridges_evms.flows`.
- A deposit requires a successful decoded Aero execution, an actual `transferRemote(uint32,bytes32,uint256)` call **from** the Metarouter to an allowlisted USDC router, and the matching canonical mailbox dispatch log. A failed optional command or reverted ancestor has no persisted dispatch and cannot become a deposit.
- A withdrawal requires a canonical mailbox `ProcessId` for a known Aero deposit and a destination-router `ReceivedTransferRemote` for the same origin, recipient and amount. `EXISTS` avoids multiplying rows when a transaction processes several messages.
- The deposit ledger is incremental and retained across runs. The withdrawal lookup deliberately reads its full history so settlements delayed beyond the incremental lookback remain attributable. Pending deposits appear through the existing full outer join in `flows`.
- The first observed Metarouter executions were 2026-10-21/22. The initial scan begins 2026-10-01, before those deployments' observed activity.

This version excludes governance messages, xAERO redemptions, non-USDC assets, `transferRemoteTo` cross-collateral routes, same-chain transfers, and destinations outside the six-chain allowlist. Robinhood's Metarouter is decoded, but no route was established for it in this integration. The live seven-day command search found the six USDC routes above; this is not a claim of exhaustive future asset coverage.

## Validation and deployment

[Compiled SQL round-trip fixture](https://dune.com/queries/9057881): Optimism transaction `0x9bc92a7c4c5deb3ac6e765b668fd2ad9786c4cb90a0ec4584bcf61fb34f5dec7`, source log 83, matches Base transaction `0x1bee37baf393ed3c0e20377bc808d8bf32bf21cbb7413efadc0af63cb3a9994f`, destination log 428. Both legs have message `0xa421413a7f8369907f9604b36688009f8e3abf5d4eeca1cfdd5d8ad1f694634a` and 45,682,762 raw USDC units. These independently observed rows are regression seeds, with uniqueness and non-null tests on ledger keys.

[Six-chain source fixtures](https://dune.com/queries/9057890) returned six distinct deposits. [Destination checks](https://dune.com/queries/9057899) matched five of those messages; the Arc source fixture remains unmatched in this check. This is sample validation, not a claim that every route or pending transfer has been verified.

Local validation: `uv run dbt compile --select '*aero*' bridges_arc_deposits bridges_arc_withdrawals bridges_evms_deposits_raw bridges_evms_withdrawals_raw --project-dir dbt_subprojects/hourly_spellbook/ --profiles-dir dbt_subprojects/hourly_spellbook/ --no-introspect --no-populate-cache`.

CI must build and run the seed/data tests before merge. Production must backfill the new ledgers and affected chain unions, then the enriched `bridges_evms.deposits` / `withdrawals` tables; their normal incremental windows alone cannot import the full Aero history. Existing sector schemas and the `flows` join are unchanged. Benchmark the full historical and incremental builds: an initial unanchored seven-day raw-trace probe timed out on the small engine; the final model adds the decoded-execution transaction filter. No production build or backfill has been performed.
