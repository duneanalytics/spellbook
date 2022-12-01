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
    'bnb' as blockchain,
    date_trunc('day', block_time) AS block_date,
    block_number,
    block_time,
    txns.hash AS tx_hash,
    txns.from AS tx_sender, 
    txns.to AS tx_receiver,
    'BNB' as native_token_symbol,
    CAST(value AS DOUBLE)/1e18 AS tx_amount_native,
    CAST(value AS DOUBLE)/1e18 * p.price AS tx_amount_usd,
    (CAST(CAST(gas_price AS DOUBLE) AS DOUBLE) * CAST(txns.gas_used AS DOUBLE))/1e18 AS tx_fee_native, 
    (CAST(CAST(gas_price AS DOUBLE) AS DOUBLE) * CAST(txns.gas_used AS DOUBLE))/1e18 * p.price  AS tx_fee_usd,
    CASE WHEN block_number >= 13082000 AND txns.to = '0x0000000000000000000000000000000000001000' THEN CAST(value AS DOUBLE)/1e18 * 10 / 100 
        ELSE NULL::double END AS burned_native, -- change after BEP95
    CASE WHEN block_number >= 13082000 AND txns.to = '0x0000000000000000000000000000000000001000' THEN CAST(value AS DOUBLE)/1e18 * 10 / 100 * p.price 
        ELSE NULL::double END AS burned_usd, -- change after BEP95
    miner AS validator,
    CAST(CAST(gas_price AS DOUBLE) AS DOUBLE) /1e9 AS CAST(CAST(gas_price AS DOUBLE) AS DOUBLE)_gwei,
    CAST(CAST(gas_price AS DOUBLE) AS DOUBLE) / 1e18 * p.price AS CAST(CAST(gas_price AS DOUBLE) AS DOUBLE)_usd,
    CAST(txns.gas_used AS DOUBLE),
    CAST(txns.gas_used AS DOUBLE) / CAST(txns.gas_limit AS DOUBLE) * 100 AS gas_usage_percent,
    CAST(txns.gas_limit AS DOUBLE),
    difficulty,
    type AS transaction_type
FROM {{ source('bnb','transactions') }} txns
JOIN {{ source('bnb','blocks') }} blocks ON blocks.number = txns.block_number
{% if is_incremental() %}
AND block_time >= date_trunc("day", now() - interval '2 days')
AND blocks.time >= date_trunc("day", now() - interval '2 days')
{% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', block_time)
AND p.blockchain = 'ethereum'
AND p.symbol = 'BNB'
{% if is_incremental() %}
AND p.minute >= date_trunc("day", now() - interval '2 days')
WHERE block_time >= date_trunc("day", now() - interval '2 days')
AND blocks.time >= date_trunc("day", now() - interval '2 days')
AND p.minute >= date_trunc("day", now() - interval '2 days')
{% endif %}