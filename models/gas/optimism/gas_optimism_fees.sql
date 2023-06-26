{{ config(
    alias = 'fees',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','block_number']
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
     txns.gas_price/1e9 as gas_price_gwei,
     txns.gas_price/1e18 * p.price as gas_price_usd,
     txns.gas_used as gas_used,
     txns.gas_limit as gas_limit,
     txns.gas_used / txns.gas_limit * 100 as gas_usage_percent,
     l1_gas_price/1e9 as l1_gas_price_gwei,
     l1_gas_price/1e18 * p.price as l1_gas_price_usd,
     l1_gas_used,
     l1_fee_scalar,
     (l1_gas_price * txns.gas_used)/1e18 as tx_fee_equivalent_on_l1_native,
     (l1_gas_price * txns.gas_used)/1e18 * p.price as tx_fee_equivalent_on_l1_usd,
     (length( decode(unhex(substring(data,3)), 'US-ASCII') ) - length(replace(decode(unhex(substring(data,3)), 'US-ASCII') , chr(0), ''))) as num_zero_bytes,
     (length( replace(decode(unhex(substring(data,3)), 'US-ASCII'), chr(0), '')) ) as num_nonzero_bytes,
     16 * (length( replace( decode(unhex(substring(data,3)), 'US-ASCII') , chr(0), ''))) --16 * nonzero bytes
     + 4 * ( length( decode(unhex(substring(data,3)), 'US-ASCII') ) - length(replace( decode(unhex(substring(data,3)), 'US-ASCII') , chr(0), '')) ) --4 * zero bytes
     as calldata_gas,
     type as transaction_type,
     l1_fee/1e18 AS l1_data_fee_native,
     p.price * l1_fee/1e18 AS l1_data_fee_usd,
      --use gas price pre-Bedrock (no base fee)
     (COALESCE(blocks.base_fee_per_gas,txns.gas_price)*txns.gas_used)/1e18 AS l2_base_fee_native,
     p.price * (COALESCE(blocks.base_fee_per_gas,txns.gas_price)*txns.gas_used)/1e18 AS l2_base_fee_usd,
      --base_fee_per_gas was null pre-bedrock when there was no base fee
     case when (txns.gas_price = 0) or (blocks.base_fee_per_gas IS NULL)then 0 else
        cast( (txns.gas_price-blocks.base_fee_per_gas)*txns.gas_used as double) / 1e18
     end AS l2_priority_fee_native,
     case when (txns.gas_price = 0) or (blocks.base_fee_per_gas IS NULL)then 0 else
        p.price * cast( (txns.gas_price-blocks.base_fee_per_gas)*txns.gas_used as double) / 1e18
     end AS l2_priority_fee_usd

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
