{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'beacon_fees',
    materialized = 'incremental',
    unique_key = ['beacon_slot_number', 'blob_index']
) }}
-- incremental, unless mapping hash changes, then full refresh

-- mapping hash
WITH latest_hash AS (
    SELECT hash_value 
    FROM {{ ref('rollup_economics_ethereum_mapping_hash') }} 
    ORDER BY updated_at DESC 
    LIMIT 1
),
current_mapping_hash AS (
    SELECT 
        md5(to_utf8(
            '[' || array_join(
                array_agg(distinct json_format(
                    json_parse(json_array(
                        coalesce(l2, ''), 
                        coalesce(name, ''), 
                        coalesce(settlement_layer, ''), 
                        coalesce(to_hex(from_address), '0x'), 
                        coalesce(to_hex(to_address), '0x'), 
                        coalesce(to_hex(method), '0x'), 
                        coalesce(namespace, '')
                    )))
                ), 
                ','
            ) || ']'
        )) AS hash_value
    FROM {{ source("growthepie", "l2economics_mapping", database="dune") }}
    WHERE settlement_layer = 'beacon'
)

-- main query
WITH L1_methods AS (
    SELECT 
        hash,
        bytearray_substring("data", 1, 4) AS method,
        "to" AS to_address
    FROM {{ ref('ethereum_transactions')}}
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
FROM {{ ref('ethereum_blobs')}} b 
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

-- Only refresh query if mapping hash has changed, else be an incremental table
WHERE (SELECT hash_value FROM latest_hash) IS DISTINCT FROM (SELECT hash_value FROM current_mapping_hash)
{% if is_incremental() %}
AND {{ incremental_predicate('b.beacon_slot_time') }}
{% endif %}
