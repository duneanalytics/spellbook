{{ config(
    schema = 'gas_bnb',
    alias = 'fees',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash']
    )
}}

SELECT 
    'bnb' as blockchain,
    date_trunc('day', block_time) AS block_date,
    CAST(date_trunc('month', block_time) AS DATE) AS block_month,
    block_number,
    block_time,
    txns.hash AS tx_hash,
    txns."from" AS tx_sender, 
    txns.to AS tx_receiver,
    'BNB' as native_token_symbol,
    value/1e18 AS tx_amount_native,
    value/1e18 * p.price AS tx_amount_usd,
    gas_price / 1e18 * txns.gas_used AS tx_fee_native, 
    gas_price / 1e18 * txns.gas_used * p.price  AS tx_fee_usd,
    CASE WHEN block_number >= 13082000 AND txns.to = 0x0000000000000000000000000000000000001000 THEN value/1e18 * 10 / 100 
        ELSE cast(NULL as double) END AS burned_native, -- change after BEP95
    CASE WHEN block_number >= 13082000 AND txns.to = 0x0000000000000000000000000000000000001000 THEN value/1e18 * 10 / 100 * p.price 
        ELSE cast(NULL as double) END AS burned_usd, -- change after BEP95
    miner AS validator,
    gas_price /1e9 AS gas_price_gwei,
    gas_price / 1e18 * p.price AS gas_price_usd,
    txns.gas_used,
    CASE 
        WHEN txns.gas_limit = 0 THEN NULL
        WHEN txns.gas_limit != 0 THEN txns.gas_used / txns.gas_limit * 100
    END AS gas_usage_percent,
    txns.gas_limit,
    difficulty,
    type AS transaction_type
FROM {{ source('bnb','transactions') }} txns
JOIN {{ source('bnb','blocks') }} blocks ON blocks.number = txns.block_number
{% if is_incremental() %}
AND block_time >= date_trunc('day', now() - interval '2' day)
AND blocks.time >= date_trunc('day', now() - interval '2' day)
{% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', block_time)
AND p.blockchain = 'ethereum'
AND p.symbol = 'BNB'
{% if is_incremental() %}
AND p.minute >= date_trunc('day', now() - interval '2' day)
WHERE block_time >= date_trunc('day', now() - interval '2' day)
AND blocks.time >= date_trunc('day', now() - interval '2' day)
AND p.minute >= date_trunc('day', now() - interval '2' day)
{% endif %}