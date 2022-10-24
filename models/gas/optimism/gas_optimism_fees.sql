{{ config(
    alias = 'fees',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash']
    )
}}

SELECT 
     'optimism' as blockchain,
     date_trunc('day', block_time) AS block_date,
     block_number,
     block_time,
     txns.hash AS tx_hash,
     txns.from AS tx_sender, 
     txns.to AS tx_receiver,
     'ETH' as native_token_symbol,
     value/1e18 AS tx_amount_native,
     value/1e18 * p.price AS tx_amount_usd,
     (l1_fee + (txns.gas_used * txns.gas_price))/1e18 as tx_fee_native, 
     (l1_fee + (txns.gas_used * txns.gas_price))/1e18 * p.price AS tx_fee_usd,
     cast(NULL as double) AS burned_native, -- Not applicable for L2s
     cast(NULL as double) AS burned_usd, -- Not applicable for L2s
     cast(NULL as string) as validator, -- Not applicable for L2s
     l1_gas_price/1e9 as gas_price_gwei,
     l1_gas_price/1e18 * p.price as gas_price_usd,
     txns.gas_used,
     l1_fee_scalar,
     txns.gas_price/1e9 as l2_gas_price_gwei,
     txns.gas_price/1e18 * p.price as l2_gas_price_usd,
     txns.gas_used as l2_gas_used,
     cast(NULL as bigint) as gas_limit, --Not applicable for L2s
     cast(NULL as double) as gas_usage_percent, --Not applicable for L2s
     type AS transaction_type
FROM {{ source('optimism','transactions') }} txns
JOIN {{ source('optimism','blocks') }} blocks ON blocks.number = txns.block_number
{% if is_incremental() %}
AND block_time >= date_trunc("day", now() - interval '2 days')
AND blocks.time >= date_trunc("day", now() - interval '2 days')
{% endif %}
LEFT JOIN {{ source('prices','usd') }} p ON p.minute = date_trunc('minute', block_time)
AND p.blockchain = 'optimism'
AND p.symbol = 'WETH'
{% if is_incremental() %}
AND p.minute >= date_trunc("day", now() - interval '2 days')
WHERE block_time >= date_trunc("day", now() - interval '2 days')
AND blocks.time >= date_trunc("day", now() - interval '2 days')
AND p.minute >= date_trunc("day", now() - interval '2 days')
{% endif %}