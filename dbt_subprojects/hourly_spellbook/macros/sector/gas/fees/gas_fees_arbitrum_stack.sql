{% macro gas_fees_arbitrum_stack(
    blockchain = 'arbitrum'
    ,token_symbol = 'ETH') %}
-- Arbitrum stack chains expose a effective_gas_price column that should be used in the calculations

SELECT
     '{{blockchain}}' as blockchain
     ,CAST(date_trunc('month', block_time) AS DATE) AS block_month
     ,CAST(date_trunc('day', block_time) AS DATE) AS block_date
     ,block_time
     ,block_number
     ,txns.hash AS tx_hash
     ,txns."from" AS tx_sender
     ,txns.to AS tx_receiver
     ,'{{token_symbol}}' as native_token_symbol
     ,value/1e18 AS tx_amount_native
     ,value/1e18 * p.price AS tx_amount_usd
     ,(effective_gas_price / 1e18) * txns.gas_used as tx_fee_native
     ,(effective_gas_price / 1e18) * txns.gas_used * p.price AS tx_fee_usd
     ,txns.effective_gas_price/1e9 as gas_price_gwei
     ,txns.effective_gas_price/1e18 * p.price as gas_price_usd
     ,txns.gas_price/1e9 as gas_price_bid_gwei
     ,txns.gas_price/1e18 * p.price as gas_price_bid_usd
     ,txns.gas_used as gas_used
     ,txns.gas_limit as gas_limit
     ,CASE
        WHEN txns.gas_limit = 0 THEN NULL
        WHEN txns.gas_limit != 0 THEN txns.gas_used / txns.gas_limit * 100
     END AS gas_usage_percent
     ,gas_used_for_l1 as l1_gas_used
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
    {% else %}
    AND p.minute >= date '2024-08-01'
    {% endif %}
{% if is_incremental() %}
WHERE {{ incremental_predicate('txns.block_time') }}
{% else %}
WHERE txns.block_time >= date '2024-08-01'
{% endif %}
{% endmacro %}