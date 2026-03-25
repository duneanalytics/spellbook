# Product Requirement Document

## [Sui Curated Transfers - PRD]

Authors: [Tomasz](mailto:tomasz@dune.com)  
When: Mar 15, 2026  
Status: Draft  

| Reviewer | Status | Notes |
| :---- | :---- | :---- |
| Alice | Not started |  |
| Jeff | Not started |  |
| Robin | Not started |  |
| Florian | Not started |  |

### Changelog

- Mar 15, 2026: Initial version

---

## Document intent

Define a minimal, production-facing Sui fungible transfer surface that is complete enough for general token analytics, while exposing a companion activity-link layer for tx-level attribution workflows.

## Problem statement

Sui token movement is object/state based and does not have a single universal ERC20-style transfer event stream. Event-only modeling can miss protocol and object-lifecycle edge cases, while pure activity labeling is not sufficient for canonical transfer coverage.

## Problems and solutions

1. **No universal fungible transfer event stream on Sui**  
   Build a canonical reconstructed transfer layer from object ownership and balance deltas.

2. **Need analyst attribution without breaking transfer correctness**  
   Add companion activity-leg models that map transfer rows to event-level activity hints.

3. **Token identity ambiguity**  
   Use `coin_type` as canonical token identity and retain Sui-specific owner/object context.

## Product surfaces to expose

1. `tokens_sui.base_transfers`  
   Canonical reconstructed net owner-to-owner value movement.

2. `tokens_sui.transfers`  
   Enriched transfer layer with metadata + USD valuation.

3. `tokens_sui.transfer_activity_legs_base`  
   Raw candidate transfer-to-event mappings.

4. `tokens_sui.transfer_activity_legs`  
   Enriched activity legs with confidence and allocation safety fields.

## Key product decisions

- Keep Sui standalone for now (do not union into EVM/global transfer surfaces yet).
- Keep canonical transfers and activity attribution as separate model layers.
- Prefer transfer-conservation net reconstruction as the base contract.
- Expose Sui-specific fields (`coin_type`, owner type, owner object IDs) in final outputs.

## Query patterns to support (v1)

- Transfer analytics by wallet/object owner.
- Token flow analytics with USD enrichment.
- Tx-level activity attribution by joining transfers to activity-leg models.

## Non-goals (for this phase)

- Exact command-by-command intra-tx transfer path parity for every complex PTB.
- Full protocol action taxonomy for all Sui ecosystems.
- Cross-chain harmonization of Sui transfers into EVM aggregate transfer unions.

## Data semantics and requirements

- Fungible transfer contract is reconstructed net movement, not direct emitted event parity.
- Canonical token key is `coin_type`.
- Mint/burn represented explicitly with `transfer_type`.
- Owner context retained via `from_owner_type`, `to_owner_type`, and owner object id fields when applicable.
- Activity-leg layer may be many-to-many and must provide overcount-safe allocation signals.

## Success metrics

- Canonical transfer totals reconcile at tx + coin level.
- Activity-leg outputs provide usable confidence-ranked attribution for analyst workflows.
- dbt incremental models compile successfully with schema tests on merge keys.

## Risks

- Event matching confidence varies across protocols and event schemas.
- Naive aggregation on activity legs may overcount without allocation fields.
- Some failed-tx native gas edge behavior may require follow-up handling if scope expands.

## Launch scope (v1)

Ship all four Sui models above with schema docs/tests and incremental configs, plus Sui source dependencies needed for object lineage reconstruction.
