{% set blockchain = 'viction' %}

{{ config(
    schema = 'gas_' + blockchain
    ,alias = 'fees'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy='merge'
    ,unique_key = ['block_month', 'tx_hash']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    ,tags = ['prod_exclude']
    )
}}

WITH base_fees AS (
    {{ evm_l1_gas_fees(blockchain) }}
),
ranked_fees AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY block_month, tx_hash ORDER BY block_time DESC) as rn
    FROM base_fees
)

SELECT 
    blockchain,
    block_month,
    block_date,
    block_time,
    block_number,
    tx_hash,
    tx_from,
    tx_to,
    gas_price,
    gas_used,
    currency_symbol,
    tx_fee_raw,
    tx_fee,
    tx_fee_usd,
    tx_fee_breakdown_raw,
    tx_fee_breakdown,
    tx_fee_breakdown_usd,
    tx_fee_currency,
    block_proposer,
    max_fee_per_gas,
    priority_fee_per_gas,
    max_priority_fee_per_gas,
    base_fee_per_gas,
    gas_limit,
    gas_limit_usage
FROM ranked_fees
WHERE rn = 1