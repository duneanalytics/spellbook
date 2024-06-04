{{ config(
    schema = 'gas_ethereum',
    alias = 'fees',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash']
    )
}}

SELECT 
     'ethereum' as blockchain,
     date_trunc('day', block_time) AS block_date,
     CAST(date_trunc('month', block_time) AS DATE) AS block_month,
     block_number,
     block_time,
     txns.hash AS tx_hash,
     txns."from" AS tx_sender, 
     txns.to AS tx_receiver,
     'ETH' as native_token_symbol,
     value/1e18 AS tx_amount_native,
     value/1e18 * p.price AS tx_amount_usd,
     CASE WHEN type = 'Legacy' THEN (cast(gas_price as double)/1e18 * cast(txns.gas_used as double))
          WHEN type = 'AccessList' THEN (cast(gas_price as double)/1e18 * cast(txns.gas_used as double))
          WHEN type = 'DynamicFee' THEN ((cast(base_fee_per_gas as double)/1e18 + cast(priority_fee_per_gas as double)/1e18)* cast(txns.gas_used as double)) 
          END AS tx_fee_native, 
     CASE WHEN type = 'Legacy' THEN (cast(gas_price as double)/1e18 * cast(txns.gas_used as double)) * p.price 
          WHEN type = 'AccessList' THEN (cast(gas_price as double)/1e18 * cast(txns.gas_used as double)) * p.price 
          WHEN type = 'DynamicFee' THEN ((cast(base_fee_per_gas as double)/1e18 + cast(priority_fee_per_gas as double)/1e18)* cast(txns.gas_used as double))  * p.price 
          END AS tx_fee_usd,
     base_fee_per_gas / 1e18 * txns.gas_used AS burned_native, 
     base_fee_per_gas / 1e18 * txns.gas_used * p.price AS burned_usd,
     (max_fee_per_gas - priority_fee_per_gas - base_fee_per_gas) / 1e18 * txns.gas_used AS tx_savings_native,
     ((max_fee_per_gas - priority_fee_per_gas - base_fee_per_gas) /1e18 * txns.gas_used) * p.price AS tx_savings_usd,
     miner AS validator, -- or block_proposer since Proposer Builder Separation (PBS) happened ?
     max_fee_per_gas / 1e9 AS max_fee_gwei,
     max_fee_per_gas / 1e18 * p.price AS max_fee_usd,
     base_fee_per_gas / 1e9 AS base_fee_gwei,
     base_fee_per_gas / 1e18 * p.price AS base_fee_usd,
     priority_fee_per_gas / 1e9 AS priority_fee_gwei,
     priority_fee_per_gas / 1e18 * p.price AS priority_fee_usd,
     gas_price /1e9 AS gas_price_gwei,
     gas_price / 1e18 * p.price AS gas_price_usd,
     txns.gas_used,
     txns.gas_limit,
     CASE 
        WHEN txns.gas_limit = 0 THEN NULL
        WHEN txns.gas_limit != 0 THEN txns.gas_used / txns.gas_limit * 100
     END AS gas_usage_percent,
     difficulty,
     type AS transaction_type
FROM {{ source('ethereum','transactions') }} txns
JOIN {{ source('ethereum','blocks') }} blocks ON blocks.number = txns.block_number
{% if is_incremental() %}
AND block_time >= date_trunc('day', now() - interval '2' day)
AND blocks.time >= date_trunc('day', now() - interval '2' day)
{% endif %}
LEFT JOIN {{ source('prices','usd') }} p ON p.minute = date_trunc('minute', block_time)
AND p.blockchain = 'ethereum'
AND p.symbol = 'WETH'
{% if is_incremental() %}
AND p.minute >= date_trunc('day', now() - interval '2' day)
WHERE block_time >= date_trunc('day', now() - interval '2' day)
AND blocks.time >= date_trunc('day', now() - interval '2' day)
AND p.minute >= date_trunc('day', now() - interval '2' day)
{% endif %}