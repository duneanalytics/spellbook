{{ config(
    schema = 'gas_gnosis',
    alias = 'fees',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['tx_hash']
    )
}}

SELECT
     'gnosis' as blockchain,
     date_trunc('day', txns.block_time) AS block_date,
     CAST(date_trunc('month', txns.block_time) AS DATE) AS block_month,
     txns.block_number,
     txns.block_time,
     txns.hash AS tx_hash,
     txns."from" AS tx_sender,
     txns.to AS tx_receiver,
     'ETH' as native_token_symbol,
     txns.value/1e18 AS tx_amount_native,
     txns.value/1e18 AS tx_amount_usd,
     CASE WHEN type = 'Legacy' THEN (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double))
          WHEN type = 'AccessList' THEN (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double))
          WHEN type = 'DynamicFee' THEN ((cast(blocks.base_fee_per_gas as double)/1e18 + cast(txns.priority_fee_per_gas as double)/1e18)* cast(txns.gas_used as double))
       -- TODO:  source named 'gnosis.blobs_submissions' which was not found
--           WHEN type = '3' THEN ((cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double)) + (cast(blob.blob_base_fee as double)/1e18 * cast(blob.blob_gas_used as double)))
          END AS tx_fee_native,
     CASE WHEN type = 'Legacy' THEN (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double))
          WHEN type = 'AccessList' THEN (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double))
          WHEN type = 'DynamicFee' THEN ((cast(blocks.base_fee_per_gas as double)/1e18 + cast(txns.priority_fee_per_gas as double)/1e18)* cast(txns.gas_used as double))
       -- TODO: source named 'gnosis.blobs_submissions' which was not found
--           WHEN type = '3' THEN ((cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double)) + (cast(blob.blob_base_fee as double)/1e18 * cast(blob.blob_gas_used as double)))
          END AS tx_fee_usd,
     blocks.base_fee_per_gas / 1e18 * txns.gas_used AS burned_native,
     blocks.base_fee_per_gas / 1e18 * txns.gas_used AS burned_usd,
     (txns.max_fee_per_gas - txns.priority_fee_per_gas - blocks.base_fee_per_gas) / 1e18 * txns.gas_used AS tx_savings_native,
     ((txns.max_fee_per_gas - txns.priority_fee_per_gas - blocks.base_fee_per_gas) /1e18 * txns.gas_used) AS tx_savings_usd,
     blocks.miner AS validator, -- or block_proposer since Proposer Builder Separation (PBS) happened ?
     txns.max_fee_per_gas / 1e9 AS max_fee_gwei,
     txns.max_fee_per_gas / 1e18 AS max_fee_usd,
     blocks.base_fee_per_gas / 1e9 AS base_fee_gwei,
     blocks.base_fee_per_gas / 1e18 AS base_fee_usd,
     txns.priority_fee_per_gas / 1e9 AS priority_fee_gwei,
     txns.priority_fee_per_gas / 1e18 AS priority_fee_usd,
     txns.gas_price /1e9 AS gas_price_gwei,
     txns.gas_price / 1e18 AS gas_price_usd,
     txns.gas_used,
     txns.gas_limit,
     CASE
        WHEN txns.gas_limit = 0 THEN NULL
        WHEN txns.gas_limit != 0 THEN txns.gas_used / txns.gas_limit * 100
     END AS gas_usage_percent,
     blocks.difficulty,
     txns.type AS transaction_type
FROM {{ source('gnosis', 'transactions') }} txns
INNER JOIN {{ source('gnosis', 'blocks') }} blocks
    ON txns.block_number = blocks.number
    {% if is_incremental() %}
    AND {{ incremental_predicate('blocks.time') }}
    {% endif %}
{% if is_incremental() %}
WHERE {{ incremental_predicate('txns.block_time') }}
{% endif %}
