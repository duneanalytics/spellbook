{% set blockchain = 'hedera' %}

-- KNOWN LIMITATION: tx_fee = gas_price * gas_used over-estimates the actual
-- charged fee for AccessList / contract-call txs (gas_price is the user's bid,
-- not the effective rate). Legacy HBAR transfers match exactly. Switch to the
-- parent record's charged_tx_fee once it is exposed on hedera.transactions.
-- See CUR2-493 for context.

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
