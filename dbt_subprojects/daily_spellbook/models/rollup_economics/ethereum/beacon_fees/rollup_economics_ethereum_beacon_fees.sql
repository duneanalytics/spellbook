{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'beacon_fees',
    materialized = 'table',
    full_refresh = true,
    unique_key = ['beacon_slot_number', 'blob_index']
) }}

WITH L1_methods AS (
    SELECT 
        hash,
        bytearray_substring("data", 1, 4) AS method,
        "to" AS to_address
    FROM {{ source('ethereum', 'transactions')}}
    WHERE 
        TYPE = '3'
        AND block_time >= TIMESTAMP '2024-03-13' -- EIP-4844 launch date
)

SELECT
    q.name AS name,
    q.l2 AS origin_key,
    CAST(date_trunc('month', b.beacon_slot_time) AS date) AS beacon_slot_month,
    CAST(date_trunc('day', b.beacon_slot_time) AS date) AS beacon_slot_day,
    b.beacon_slot_time,
    b.beacon_slot_number,
    b.blob_index,
    b.block_number,
    b.beacon_epoch,
    b.blob_kzg_commitment,
    b.blob_versioned_hash,
    b.tx_hash,
    b.blob_submitter AS from_address,
    m.to_address,
    m.method,
    b.used_blob_byte_count, -- only count the actual used space within a fixed-size blob
    b.blob_gas_used,
    b.blob_base_fee,
    (CAST(b.blob_gas_used AS double) * CAST(b.blob_base_fee AS double) / 1e18) / (b.used_blob_byte_count) AS fee_native_per_byte,
    ((CAST(b.blob_gas_used AS double) * CAST(b.blob_base_fee AS double) / 1e18) * p.price) / (b.used_blob_byte_count) AS fee_usd_per_byte,
    CAST(b.blob_gas_used AS double) * CAST(b.blob_base_fee AS double) / 1e18 AS fee_native,
    (CAST(b.blob_gas_used AS double) * CAST(b.blob_base_fee AS double) / 1e18) * p.price AS fee_usd
FROM {{ source('ethereum', 'blobs')}} b 
LEFT JOIN L1_methods m ON b.tx_hash = m.hash
JOIN (
    SELECT * 
    FROM {{ source("growthepie", "l2economics_mapping", database="dune") }} -- update mapping here https://github.com/growthepie/gtp-dna/tree/main/economics_da
    WHERE settlement_layer = 'beacon'
) q
    ON (q.from_address IS NULL OR q.from_address = b.blob_submitter) 
    AND (q.to_address IS NULL OR q.to_address = m.to_address) 
    AND (q.method IS NULL OR q.method = m.method)
INNER JOIN {{ source('prices', 'usd') }} p
    ON p."minute" = date_trunc('minute', b.beacon_slot_time)
    AND p.blockchain IS NULL
    AND p.symbol = 'ETH'
    AND p.minute >= TIMESTAMP '2024-03-13' -- EIP-4844 launch date