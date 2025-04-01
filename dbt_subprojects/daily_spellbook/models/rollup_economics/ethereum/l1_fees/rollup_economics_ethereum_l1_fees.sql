{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'l1_fees',
    materialized = 'table',
    full_refresh = true,
    unique_key = ['origin_key', 'tx_hash']
) }}

SELECT 
    q.name AS name,
    q.l2 AS origin_key,
    CAST(date_trunc('month', t.block_time) AS date) AS block_time_month,
    CAST(date_trunc('day', t.block_time) AS date) AS block_time_day,
    t.block_time,
    t.block_number,
    t."type",
    t.nonce,
    t.index,
    t.success,
    t.block_hash,
    t.hash AS tx_hash,
    t."from" AS from_address,
    t."to" AS to_address,
    bytearray_substring(t."data", 1, 4) AS method,
    t.gas_price,
    t.gas_used,
    t.max_fee_per_gas,
    t.max_priority_fee_per_gas,
    t.priority_fee_per_gas,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used,
    LENGTH(t."data") AS data_length,
    CAST(t.gas_used AS double) * CAST(t.gas_price AS double) / 1e18 AS fee_native,
    (CAST(t.gas_used AS double) * CAST(t.gas_price AS double) / 1e18) * p.price AS fee_usd
FROM {{ source('ethereum', 'transactions') }} t
JOIN (
    SELECT * 
    FROM {{ source("growthepie", "l2economics_mapping", database="dune") }} -- update mapping here https://github.com/growthepie/gtp-dna/tree/main/economics_da
    WHERE settlement_layer = 'l1'
) q
    ON (q.from_address IS NULL OR t."from" = q.from_address) 
    AND (q.to_address IS NULL OR t."to" = q.to_address) 
    AND (q.method IS NULL OR bytearray_substring(t."data", 1, 4) = q.method)
INNER JOIN {{ source('prices', 'usd') }} p
    ON p."minute" = date_trunc('minute', t.block_time)
    AND p.blockchain IS NULL
    AND p.symbol = 'ETH'
    AND p.minute >= TIMESTAMP '2020-01-01' -- L2s development started around this time
WHERE t.block_time >= TIMESTAMP '2020-01-01' -- L2s development started around this time