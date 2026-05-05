{% set blockchain = 'hedera' %}

-- KNOWN LIMITATION: tx_fee = gas_price * gas_used does NOT match the actual
-- HBAR debited from the sender for storage-modifying contract calls on Hedera.
--   - Legacy txs (simple HBAR transfers): matches HashScan exactly.
--   - AccessList / contract-call txs: over-estimates by up to ~20%.
-- The authoritative debited amount is `charged_tx_fee` on the parent Hedera
-- transaction record (Mirror Node), but this field is not currently exposed
-- on `hedera.transactions`. Verified example: tx
-- 0xc3c133f775b6fde6bcd851681c7cd572271e5d5ee24c1df662849bd62131b92f
--   model:   0.29066 HBAR   (gas_price * gas_used)
--   debited: 0.24011 HBAR   (Mirror Node charged_tx_fee, ~17.4% lower)
-- Switch to charged_tx_fee once the indexer surfaces it. See CUR2-493.

{{ config(
    schema = 'gas_' + blockchain
    ,alias = 'fees'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy='merge'
    ,unique_key = ['block_month', 'tx_hash']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{ evm_l1_gas_fees(blockchain) }}
