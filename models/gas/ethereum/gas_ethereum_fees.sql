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
     'ethereum' as blockchain,
     date_trunc('day', block_time) AS block_date,
     block_number,
     block_time,
     txns.hash AS tx_hash,
     txns."from" AS tx_sender, 
     txns.to AS tx_receiver,
     'ETH' as native_token_symbol,
     CAST(value AS DOUBLE)/1e18 AS tx_amount_native,
     CAST(value AS DOUBLE)/1e18 * p.price AS tx_amount_usd,
     CASE WHEN type = 'Legacy' THEN (CAST(gas_price AS DOUBLE) * CAST(txns.gas_used AS double))/1e18
          WHEN type = 'DynamicFee' THEN ((CAST(base_fee_per_gas AS DOUBLE) + CAST(priority_fee_per_gas AS double)) * 
                                         CAST(txns.gas_used AS DOUBLE))/1e18 END AS tx_fee_native, 
     CASE WHEN type = 'Legacy' THEN (CAST(gas_price AS DOUBLE) * CAST(txns.gas_used AS double))/1e18 * p.price
          WHEN type = 'DynamicFee' THEN  ((CAST(base_fee_per_gas AS DOUBLE) + CAST(priority_fee_per_gas AS double)) * 
                                         CAST(txns.gas_used AS DOUBLE))/1e18  * p.price 
          END AS tx_fee_usd,
     ((CAST(base_fee_per_gas AS DOUBLE)) * CAST(txns.gas_used AS DOUBLE))/1e18 AS burned_native, 
     ((CAST(base_fee_per_gas AS DOUBLE)) * CAST(txns.gas_used AS DOUBLE))/1e18 * p.price AS burned_usd,
     ((CAST(max_fee_per_gas AS DOUBLE) -  CAST(priority_fee_per_gas AS DOUBLE) - CAST(base_fee_per_gas AS DOUBLE)) * CAST(txns.gas_used AS DOUBLE))/1e18 AS tx_savings_native,
     (((CAST(max_fee_per_gas AS DOUBLE) -  CAST(priority_fee_per_gas AS DOUBLE) - CAST(base_fee_per_gas AS DOUBLE)) * CAST(txns.gas_used AS DOUBLE))/1e18) * p.price AS tx_savings_usd,
     miner AS validator, -- or block_proposer since Proposer Builder Separation (PBS) happened ?
     CAST(max_fee_per_gas AS DOUBLE) / 1e9 AS max_fee_gwei,
     CAST(max_fee_per_gas AS DOUBLE) / 1e18 * p.price AS max_fee_usd,
     CAST(base_fee_per_gas AS DOUBLE) / 1e9 AS base_fee_gwei,
     CAST(base_fee_per_gas AS DOUBLE) / 1e18 * p.price AS base_fee_usd,
     CAST(priority_fee_per_gas AS DOUBLE) / 1e9 AS priority_fee_gwei,
     CAST(priority_fee_per_gas AS DOUBLE) / 1e18 * p.price AS priority_fee_usd,
     CAST(gas_price AS DOUBLE) /1e9 AS gas_price_gwei,
     CAST(gas_price AS DOUBLE) / 1e18 * p.price AS gas_price_usd,
     CAST(txns.gas_used AS DOUBLE) AS gas_used,
     CAST(txns.gas_limit AS DOUBLE) AS gas_limit,
     CAST(txns.gas_used AS DOUBLE) / CAST(txns.gas_limit AS DOUBLE) * 100 AS gas_usage_percent,
     difficulty,
     type AS transaction_type
FROM {{ source('ethereum','transactions') }} txns
JOIN {{ source('ethereum','blocks') }} blocks ON blocks.number = txns.block_number
{% if is_incremental() %}
AND block_time >= date_trunc("day", now() - interval '2 days')
AND blocks.time >= date_trunc("day", now() - interval '2 days')
{% endif %}
LEFT JOIN {{ source('prices','usd') }} p ON p.minute = date_trunc('minute', block_time)
AND p.blockchain = 'ethereum'
AND p.symbol = 'WETH'
{% if is_incremental() %}
AND p.minute >= date_trunc("day", now() - interval '2 days')
WHERE block_time >= date_trunc("day", now() - interval '2 days')
AND blocks.time >= date_trunc("day", now() - interval '2 days')
AND p.minute >= date_trunc("day", now() - interval '2 days')
{% endif %}