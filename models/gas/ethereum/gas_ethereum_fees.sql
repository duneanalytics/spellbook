{{ config(
    alias = 'fees',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time','tx_hash','tx_amount_eth']
    )
}}

SELECT 
     block_time,
     block_number,
     txns.hash AS tx_hash,
     value/1e18 AS tx_amount_eth,
     value/1e18 * p.price AS tx_amount_usd,
     CASE WHEN type = 'Legacy' THEN (gas_price * txns.gas_used)/1e18
          WHEN type = 'DynamicFee' THEN ((base_fee_per_gas + priority_fee_per_gas) * txns.gas_used)/1e18 
          END AS tx_fee_eth, 
     CASE WHEN type = 'Legacy' THEN (gas_price * txns.gas_used)/1e18 * p.price 
          WHEN type = 'DynamicFee' THEN ((base_fee_per_gas + priority_fee_per_gas) * txns.gas_used)/1e18 * p.price 
          END AS tx_fee_usd,
     CASE WHEN type = 'Legacy' THEN NULL::double 
          WHEN type = 'DynamicFee' THEN ((base_fee_per_gas) * txns.gas_used)/1e18 
          END AS burned_eth, 
     CASE WHEN type = 'Legacy' THEN NULL::double 
          WHEN type = 'DynamicFee' THEN (((base_fee_per_gas) * txns.gas_used)/1e18) * p.price 
          END AS burned_usd,
     CASE WHEN type = 'Legacy' THEN NULL::double 
          WHEN type = 'DynamicFee' THEN ((max_fee_per_gas - priority_fee_per_gas - base_fee_per_gas) * txns.gas_used)/1e18 
          END AS tx_savings_eth,
     CASE WHEN type = 'Legacy' THEN NULL::double 
          WHEN type = 'DynamicFee' THEN (((max_fee_per_gas - priority_fee_per_gas - base_fee_per_gas) * txns.gas_used)/1e18) * p.price 
          END AS tx_savings_usd,
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
     txns.gas_used / txns.gas_limit * 100 AS gas_usage_percent,
     difficulty,
     type AS transaction_type
FROM ethereum.transactions txns
JOIN ethereum.blocks blocks ON blocks.number = txns.block_number
{% if is_incremental() %}
AND txns.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', block_time)
AND p.blockchain = 'ethereum'
AND p.symbol = 'WETH'
{% if is_incremental() %}
AND p.minute >= date_trunc("day", now() - interval '1 week')
{% endif %}