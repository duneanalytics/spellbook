{% macro gas_fees_frontier(
    blockchain = 'ethereum'
    ,token_symbol = 'ETH') %}
-- frontier is the initial ethereum network release

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
     ,cast(txns.gas_price as uint256) * cast(txns.gas_used as uint256) AS tx_fee_raw
     ,cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double) AS tx_fee_native
     ,cast(txns.gas_price as double)/1e18 * cast(txns.gas_used as double) * p.price AS tx_fee_usd
     ,blocks.miner AS validator -- or block_proposer since Proposer Builder Separation (PBS) happened ?
     ,txns.gas_price/1e9 as gas_price_gwei
     ,txns.gas_price/1e18 * p.price as gas_price_usd
     ,txns.gas_used as gas_used
     ,txns.gas_limit as gas_limit
     ,CASE
        WHEN txns.gas_limit = 0 THEN NULL
        WHEN txns.gas_limit != 0 THEN txns.gas_used / txns.gas_limit * 100
     END AS gas_usage_percent
     ,type as transaction_type
FROM {{ source( blockchain, 'transactions') }} txns
INNER JOIN {{ source( blockchain, 'blocks') }} blocks
    ON txns.block_number = blocks.number
    {% if is_incremental() %}
    AND {{ incremental_predicate('blocks.time') }}
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
