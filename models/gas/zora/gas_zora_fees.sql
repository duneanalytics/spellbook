{{ config(
    schema = 'gas_zora',
    alias = 'fees',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','block_number']
    )
}}

SELECT 
     'zora' as blockchain,
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
     (l1_fee/1e18 + ((txns.gas_used/1e18) * txns.gas_price)) as tx_fee_native, 
     (l1_fee/1e18 + ((txns.gas_used/1e18) * txns.gas_price)) * p.price AS tx_fee_usd,
     cast(NULL as double) AS burned_native, -- Not applicable for L2s
     cast(NULL as double) AS burned_usd, -- Not applicable for L2s
     cast(NULL as VARBINARY) as validator, -- Not applicable for L2s
     txns.gas_price/1e9 as gas_price_gwei,
     txns.gas_price/1e18 * p.price as gas_price_usd,
     txns.gas_used as gas_used,
     txns.gas_limit as gas_limit,
     CASE 
        WHEN txns.gas_limit = 0 THEN NULL
        WHEN txns.gas_limit != 0 THEN txns.gas_used / txns.gas_limit * 100
     END AS gas_usage_percent,
     type as transaction_type
FROM {{ source('zora','transactions') }} txns
JOIN {{ source('zora','blocks') }} blocks ON blocks.number = txns.block_number
{% if is_incremental() %}
AND {{ incremental_predicate('txns.block_time') }}
AND {{ incremental_predicate('blocks.time') }}
{% endif %}
LEFT JOIN {{ source('prices','usd') }} p ON p.minute = date_trunc('minute', block_time)
AND p.blockchain = 'zora'
AND p.symbol = 'WETH'
{% if is_incremental() %}
AND {{ incremental_predicate('p.minute') }}
WHERE {{ incremental_predicate('txns.block_time') }}
AND {{ incremental_predicate('blocks.time') }}
AND {{ incremental_predicate('p.minute') }}
{% endif %}
