{% macro gas_fees_dencun(
    blockchain = 'ethereum'
    ,token_symbol = 'ETH') %}
-- dencun upgrade introduced tx type '3' with EIP-4844 (blobs)

SELECT
     '{{blockchain}}' as blockchain
     ,CAST(date_trunc('month', txns.block_time) AS DATE) AS block_month
     ,CAST(date_trunc('day', txns.block_time) AS DATE) AS block_date
     ,txns.block_time
     ,txns.block_number
     ,txns.hash AS tx_hash
     ,txns."from" AS tx_sender
     ,txns.to AS tx_receiver
     ,'{{token_symbol}}' as native_token_symbol
     ,txns.value/1e18 AS tx_amount_native
     ,txns.value/1e18 * p.price AS tx_amount_usd
     ,CASE WHEN type = 'Legacy' THEN (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double))
          WHEN type = 'AccessList' THEN (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double))
          WHEN type = 'DynamicFee' THEN ((cast(blocks.base_fee_per_gas as double)/1e18 + cast(txns.priority_fee_per_gas as double)/1e18)* cast(txns.gas_used as double))
          WHEN type = '3' THEN ((cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double)) + (cast(blob.blob_base_fee as double)/1e18 * cast(blob.blob_gas_used as double)))
          END AS tx_fee_native
     ,CASE WHEN type = 'Legacy' THEN (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double)) * p.price
          WHEN type = 'AccessList' THEN (cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double)) * p.price
          WHEN type = 'DynamicFee' THEN ((cast(blocks.base_fee_per_gas as double)/1e18 + cast(txns.priority_fee_per_gas as double)/1e18)* cast(txns.gas_used as double))  * p.price
          WHEN type = '3' THEN ((cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double)) + (cast(blob.blob_base_fee as double)/1e18 * cast(blob.blob_gas_used as double))) * p.price
          END AS tx_fee_usd
     ,blocks.base_fee_per_gas / 1e18 * txns.gas_used AS burned_native
     ,blocks.base_fee_per_gas / 1e18 * txns.gas_used * p.price AS burned_usd
     ,(txns.max_fee_per_gas - txns.priority_fee_per_gas - blocks.base_fee_per_gas) / 1e18 * txns.gas_used AS tx_savings_native
     ,((txns.max_fee_per_gas - txns.priority_fee_per_gas - blocks.base_fee_per_gas) /1e18 * txns.gas_used) * p.price AS tx_savings_usd
     ,blocks.miner AS validator -- or block_proposer since Proposer Builder Separation (PBS) happened ?
     ,txns.max_fee_per_gas / 1e9 AS max_fee_gwei
     ,txns.max_fee_per_gas / 1e18 * p.price AS max_fee_usd
     ,blocks.base_fee_per_gas / 1e9 AS base_fee_gwei
     ,blocks.base_fee_per_gas / 1e18 * p.price AS base_fee_usd
     ,txns.priority_fee_per_gas / 1e9 AS priority_fee_gwei
     ,txns.priority_fee_per_gas / 1e18 * p.price AS priority_fee_usd
     ,txns.gas_price /1e9 AS gas_price_gwei
     ,txns.gas_price / 1e18 * p.price AS gas_price_usd
     ,txns.gas_used
     ,txns.gas_limit
     ,CASE
        WHEN txns.gas_limit = 0 THEN NULL
        WHEN txns.gas_limit != 0 THEN txns.gas_used / txns.gas_limit * 100
     END AS gas_usage_percent
     ,blocks.difficulty
     ,txns.type AS transaction_type
FROM {{ source( blockchain, 'transactions') }} txns
INNER JOIN {{ source( blockchain, 'blocks') }} blocks
    ON txns.block_number = blocks.number
    {% if is_incremental() %}
    AND {{ incremental_predicate('blocks.time') }}
    {% endif %}
LEFT JOIN {{ source( blockchain, 'blobs_submissions') }} blob
    ON txns.hash = blob.tx_hash
    {% if is_incremental() %}
    AND {{ incremental_predicate('blob.block_time') }}
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.minute = date_trunc('minute', txns.block_time)
    AND p.blockchain = null
    AND p.symbol = '{{token_symbol}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}
{% if is_incremental() %}
WHERE {{ incremental_predicate('txns.block_time') }}
{% else %}
WHERE txns.block_time >= date '2024-08-01'
OR txns.hash in (select tx_hash from {{ref('evm_gas_fees')}})
{% endif %}
{% endmacro %}
