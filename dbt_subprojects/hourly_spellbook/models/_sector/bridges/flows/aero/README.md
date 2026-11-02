# Aero bridge flows

This integration attributes **USDC bridge legs** to the deployed Aero Metarouter and feeds the existing `bridges_evms.flows` lineage. It does not report final Metaswap output tokens or recipients. The bridge recipient can be a Hyperlane interchain account. `sender` is the Metarouter that calls the bridge; `tx_from` is the transaction initiator.

## Contracts and coverage

[Aero's security documentation](https://app.aero.xyz/docs/security/) identifies the Metarouter at `0x0fEf9e88A7b76BCDBF451780C6774FE32965Ee98`. Its verified [CrosschainLib](https://github.com/dromos-labs/metadex-public/blob/8b506b450aaa9d4a9f1a77e264f2d9dd508e9ddf/V3/src/metarouter/libraries/CrosschainLib.sol) passes the bridge principal after token fees to `transferRemote`. Bridge and destination-plan messages are separate operations.

The route and local token addresses come from the [Hyperlane USDC registry](https://github.com/hyperlane-xyz/hyperlane-registry/blob/d350acb9a6ce045758be6fa610bb9e697b7fd1bc/deployments/warp_routes/USDC/eclipsemainnet-config.yaml). These routes are shared with other applications. All six routes have six token decimals and scale 1:1. Mailboxes come from the same registry's `chains/<chain>/addresses.yaml`. Hyperlane domains equal EVM chain IDs for this allowlist.

| Chain | USDC collateral router | Dune decoding check, 2026-11-02 |
| --- | --- | --- |
| Arbitrum | `0xad4350ee0f9f5b85bab115425426086ae8384ebb` | Submission processed; historical coverage not yet verified |
| Arc | `0x56a255a080584de4433d0fe6587f4c0105715589` | Submission processed; historical coverage not yet verified |
| Base | `0x37e637891a558b5b621723cbf8fc771525f280c1` | Submission processed; historical coverage not yet verified |
| Ethereum | `0xe1de9910fe71cc216490ac7fcf019e13a34481d7` | Submission processed; historical coverage not yet verified |
| Ink | `0x0de665377a5b0d390469d0ea0fcae55e0c14f4c4` | Submission processed; historical coverage not yet verified |
| Optimism | `0x02bfd67829317d666dc7dfa030f18eacc12c2cfb` | Submission processed; historical coverage not yet verified |

The Metarouter already has `aero_<chain>.metarouter_call_execute` on all six chains. `aero_multichain.metarouter_call_execute` also covers those chains and Robinhood; its discriminator is named `chain`. Live rows were checked through that namespace. The model reads only the per-chain tables to avoid duplicate counting across namespaces. The root and leaf message orchestrators and Hyperlane adapters are also decoded, but carry governance and reward-redemption messages, not the Metaswap USDC bridge leg.

All six contracts were submitted under project `hyperlane`, contract `HypERC20Collateral`, with `is_proxy=true`. The live `core_db` submission records now show `processed` for all six; this status does not certify historical backfill completion. All twelve per-chain tables resolve on DuneSQL:

- `hyperlane_<chain>.hyperc20collateral_call_transferremote`: `_amount`, `_destination`, `_recipient`, `output_messageId`.
- `hyperlane_<chain>.hyperc20collateral_evt_receivedtransferremote`: `origin`, `recipient`, `amountOrId`.

The model uses those typed fields. Canonical traces are retained to establish the actual Aero caller and canonical proxy, and mailbox logs to prove dispatch/processing. Call rows are deduplicated by message and payload before matching the outer trace by its returned message ID; the model does not assume a decoded proxy call has the same trace frame or `contract_address` as the outer call. Received events are matched to their canonical emitter by transaction hash, event index and block date.

There is no raw-payload fallback or per-chain/multichain union. Missing decoded history remains missing and must fail the existing seed tests until the backfill is complete.

## Data contract

- One deposit per persisted `DispatchId`, keyed by origin chain, source transaction hash and log index. The globally unique Hyperlane message ID matches both sides in `bridges_evms.flows`.
- A deposit requires a successful decoded Aero execution and decoded Hyperlane `transferRemote`, an actual outer call **from** the Metarouter to an allowlisted USDC router, and the matching canonical mailbox dispatch log. A failed optional command or reverted ancestor has no persisted dispatch and cannot become a deposit.
- A withdrawal requires a canonical mailbox `ProcessId` for a known Aero deposit and a decoded destination-router `ReceivedTransferRemote` for the same origin, recipient and amount, confirmed against the canonical event identity. `EXISTS` avoids multiplying rows when a transaction processes several messages.
- The deposit ledger is incremental and retained across runs. The withdrawal lookup deliberately reads its full history so settlements delayed beyond the incremental lookback remain attributable. Pending deposits appear through the existing full outer join in `flows`.
- The first observed Metarouter executions were 2026-10-21/22. The initial scan begins 2026-10-01, before those deployments' observed activity.

This version excludes governance messages, xAERO redemptions, non-USDC assets, `transferRemoteTo` cross-collateral routes, same-chain transfers, and destinations outside the six-chain allowlist. Robinhood's Metarouter is decoded, but no route was established for it in this integration. The live seven-day command search found the six USDC routes above; this is not a claim of exhaustive future asset coverage.

## Validation and deployment

[Original canonical-data round-trip fixture](https://dune.com/queries/9057881): Optimism transaction `0x9bc92a7c4c5deb3ac6e765b668fd2ad9786c4cb90a0ec4584bcf61fb34f5dec7`, source log 83, matches Base transaction `0x1bee37baf393ed3c0e20377bc808d8bf32bf21cbb7413efadc0af63cb3a9994f`, destination log 428. Both legs have message `0xa421413a7f8369907f9604b36688009f8e3abf5d4eeca1cfdd5d8ad1f694634a` and 45,682,762 raw USDC units. These independently observed rows are regression seeds, with uniqueness and non-null tests on ledger keys.

[Original canonical-data source fixtures](https://dune.com/queries/9057890) returned six distinct deposits. [Original canonical-data destination checks](https://dune.com/queries/9057899) matched all six messages, including Arc (domain ID 5042). The regression seeds include all six pairs plus the round-trip fixture above. These baseline queries established the expected seeds before the decoded-source switch; they do not verify the new decoded history. The updated sources are still backfilling, and initial historical probes returned no rows. [Decoded source fixtures](https://dune.com/queries/9058001) and [decoded destination fixtures](https://dune.com/queries/9058004) exercise the updated models and must be rerun after backfill completion. Both updated SQL projections validate on DuneSQL. The decoded destination fixture probe returned zero rows; the six-chain decoded source fixture probe timed out after two minutes on the small engine, so its historical parity and runtime remain unverified.

Local validation: `uv run dbt compile --select '*aero*' bridges_arc_deposits bridges_arc_withdrawals bridges_evms_deposits_raw bridges_evms_withdrawals_raw --project-dir dbt_subprojects/hourly_spellbook/ --profiles-dir dbt_subprojects/hourly_spellbook/ --no-introspect --no-populate-cache`.

**Backfill gate:** keep the PR in draft until the decoded tables contain every seeded transfer and the decoded-source fixture queries reproduce all six source/destination pairs. An empty result while backfilling is not a passing data test. CI must build and run the seed/data tests before merge; do not weaken or remove the fixtures to accommodate an incomplete decoder backfill. After decoder backfill completes, full-refresh both Aero ledgers and backfill the affected chain unions, then the enriched `bridges_evms.deposits` / `withdrawals` tables; their normal incremental windows alone cannot import the full Aero history. Existing sector schemas and the `flows` join are unchanged. Benchmark the full historical and incremental builds on the intended engine tier before deployment. No production build or backfill has been performed.
