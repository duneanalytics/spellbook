# Technical Design Document

## [Sui Curated Transfers - TDD]

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

## Summary

Implement a layered Sui transfer stack:

1. canonical reconstructed transfer base (`base_transfers`)  
2. enriched transfer model (`transfers`)  
3. raw transfer-activity mapping base (`transfer_activity_legs_base`)  
4. enriched transfer-activity mapping (`transfer_activity_legs`)

This split preserves transfer completeness/correctness and supports downstream tx-level activity labeling.

## Scope

- Sui-only transfer and activity-leg modeling in `dbt_subprojects/tokens/models/transfers_and_balances/sui`.
- Incremental dbt models with tested merge keys.
- Required source wiring (including Sui `transaction_objects`).

## Non-goals

- Exact command-level trace reconstruction for every complex PTB.
- Full cross-protocol semantic action taxonomy.
- EVM/global union integration at this phase.

## Architecture

- **Canonical transfer layer:** reconstruct owner-level net deltas per `transaction_digest + coin_type`.
- **Enrichment layer:** add metadata and valuation while preserving Sui-native token identity.
- **Attribution layer:** map transfers to event candidates, then enrich with confidence and allocation fields.

## Input contract

Primary sources:

- `sui.transactions`
- `sui.transaction_objects`
- `sui.objects`
- `sui.events`
- `prices_external.hour`
- `prices.trusted_tokens`
- `prices_sui_tokens`

## Output contract

### 1) `tokens_sui_base_transfers`

| Column | Type | Comment |
| :---- | :---- | :---- |
| `unique_key` | varchar | Surrogate merge key |
| `blockchain` | varchar | `sui` |
| `block_month` | date | Partition column |
| `block_date` | date | Merge key component |
| `block_time` | timestamp | Tx time |
| `block_number` | bigint | Checkpoint |
| `tx_hash` | varbinary | Digest decoded |
| `tx_digest` | varchar | Base58 digest |
| `evt_index` | integer | null in this model |
| `trace_address` | array(bigint) | null in this model |
| `token_standard` | varchar | `native`/`coin` |
| `tx_from` | varbinary | Tx sender |
| `tx_to` | varbinary | Top-level receiver (nullable) |
| `tx_index` | integer | Tx position |
| `transfer_type` | varchar | `transfer`/`mint`/`burn` |
| `from_owner_type` | varchar | Sui owner type |
| `to_owner_type` | varchar | Sui owner type |
| `from` | varbinary | Sender owner |
| `to` | varbinary | Receiver owner |
| `from_owner_object_id` | varbinary | When owner is `ObjectOwner` |
| `to_owner_object_id` | varbinary | When owner is `ObjectOwner` |
| `contract_address` | varbinary | Package address from `coin_type` |
| `amount_raw` | uint256 | Raw amount |
| `coin_type` | varchar | Canonical token ID |

### 2) `tokens_sui_transfers`

All columns from `tokens_sui_base_transfers`, plus:

| Column | Type | Comment |
| :---- | :---- | :---- |
| `symbol` | varchar | Token symbol |
| `amount` | double | Decimal-normalized amount |
| `price_usd` | double | Hourly USD price |
| `amount_usd` | double | Transfer USD amount (guarded for outliers) |

### 3) `tokens_sui_transfer_activity_legs_base`

| Column | Type | Comment |
| :---- | :---- | :---- |
| `unique_key` | varchar | Surrogate merge key |
| `blockchain` | varchar | `sui` |
| `block_month` | date | Partition column |
| `block_date` | date | Merge key component |
| `block_time` | timestamp | Event time |
| `block_number` | bigint | Checkpoint |
| `tx_hash` | varbinary | Digest decoded |
| `tx_digest` | varchar | Base58 digest |
| `transfer_unique_key` | varchar | FK to base transfers |
| `tx_from` | varbinary | Tx sender |
| `tx_index` | integer | Tx position |
| `coin_type` | varchar | Canonical token ID |
| `transfer_type` | varchar | transfer class |
| `from` | varbinary | Sender owner |
| `to` | varbinary | Receiver owner |
| `amount_raw` | uint256 | Raw amount |
| `event_index` | integer | Event ordinal |
| `event_sender` | varbinary | Event sender |
| `event_package` | varbinary | Event package |
| `event_type` | varchar | Full Move event type |
| `event_module` | varchar | Parsed module |
| `event_name` | varchar | Parsed event name |
| `event_type_params` | varchar | Type params |
| `coin_type_hint` | varchar | Parsed coin hint |
| `coin_type_in_hint` | varchar | Parsed input coin hint |
| `coin_type_out_hint` | varchar | Parsed output coin hint |
| `match_reason` | varchar | Candidate match reason |
| `event_json` | varchar | Raw event payload |

### 4) `tokens_sui_transfer_activity_legs`

All columns from `*_legs_base`, plus enrichment/safety:

| Column | Type | Comment |
| :---- | :---- | :---- |
| `token_standard` | varchar | From enriched transfers |
| `contract_address` | varbinary | Package address |
| `symbol` | varchar | Token symbol |
| `amount` | double | Decimal amount |
| `price_usd` | double | Price |
| `amount_usd` | double | USD amount |
| `match_priority` | integer | Numeric match rank |
| `match_confidence` | varchar | high/medium/low |
| `transfer_match_rank` | integer | Rank within transfer |
| `is_primary_match` | boolean | Best transfer match |
| `transfer_match_count` | bigint | # events per transfer |
| `event_match_count` | bigint | # transfers per event |
| `allocation_weight` | double | `1 / transfer_match_count` |
| `allocated_amount_usd` | double | Weighted USD amount |

## Design decisions and rationale

1. **Reconstructed transfer core vs event-only core**  
   Chosen for protocol-agnostic completeness and transfer-conservation correctness on Sui.

2. **Keep `coin_type` in final model**  
   Required because `contract_address` is not a unique token identifier on Sui.

3. **Keep owner context fields**  
   `owner_type` and owner object IDs are required for object-aware downstream analysis.

4. **Separate canonical transfer models and activity-leg models**  
   Prevents attribution heuristics from contaminating canonical transfer semantics.

5. **Base + enriched split for activity legs**  
   Keeps matching logic auditable while providing analyst-ready fields in enriched output.

6. **Deleted-object pre-lineage recovery**  
   Added to avoid send-side undercount in merge/consumption patterns.

7. **Sui stays decoupled from EVM union layers**  
   Avoids premature schema/semantics mismatch at this stage.

## Processing details

- Transfer reconstruction uses object state transitions and owner deltas.
- Native SUI gas adjustment is applied to gas owner deltas.
- Mint/burn imbalance handled with synthetic zero-address side.
- Activity matching is tx-local and heuristic via event payload/type hints.
- Enriched legs include anti-overcount fields for safe aggregation.

## Validation and testing

- Incremental model compile checks run on all Sui transfer models.
- `_schema.yml` contains:
  - `dbt_utils.unique_combination_of_columns` aligned with model unique keys
  - `not_null` on unique key columns where required
  - model/column descriptions for all four outputs

## Risks and mitigations

- **Risk:** many-to-many transfer↔event links can overcount.  
  **Mitigation:** provide `allocation_weight` and `allocated_amount_usd`.

- **Risk:** event parsing heterogeneity across protocols.  
  **Mitigation:** keep raw event payload and explicit `match_reason` fields.

- **Risk:** exact intra-tx path attribution not guaranteed.  
  **Mitigation:** preserve canonical transfer layer separately from attribution layer.
