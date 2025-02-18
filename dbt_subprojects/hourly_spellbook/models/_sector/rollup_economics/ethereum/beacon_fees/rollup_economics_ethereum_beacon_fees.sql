{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'beacon_fees',
    materialized = 'incremental',
    unique_key = ['origin_key', 'tx_hash'],
    incremental_strategy = 'delete+insert',
    on_schema_change = 'sync_all_columns',
    full_refresh = check_mapping_hash('beacon')
) }}

WITH mapping_data AS (
    SELECT *
    FROM {{ source("growthepie", "l2economics_mapping", database="dune") }}
    WHERE settlement_layer = 'beacon'
),

blob_transactions_filtered AS (
    SELECT 
        t.*,
        bytearray_substring(t."data", 1, 4) AS method
    FROM {{ source('ethereum', 'blob_transactions') }} t
    WHERE t.slot_time >= TIMESTAMP '2024-01-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('t.slot_time') }}
    {% endif %}
),

prices_filtered AS (
    SELECT p.*
    FROM {{ source('prices', 'usd') }} p
    WHERE p.blockchain IS NULL
        AND p.symbol = 'ETH'
        AND p.minute >= TIMESTAMP '2024-01-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('p.minute') }}
    {% endif %}
)

SELECT 
    q.name AS name,
    q.l2 AS origin_key,
    CAST(date_trunc('month', t.slot_time) AS date) AS beacon_slot_time_month,
    CAST(date_trunc('day', t.slot_time) AS date) AS beacon_slot_time_day,
    t.slot_time AS beacon_slot_time,
    t.slot AS beacon_slot,
    t.block_number,
    t.block_hash,
    t.tx_hash,
    t."from" AS from_address,
    t."to" AS to_address,
    t.method,
    t.blob_gas_price,
    t.blob_gas_used,
    t.used_blob_byte_count,
    CAST(t.blob_gas_used AS double) * CAST(t.blob_gas_price AS double) / 1e18 AS fee_native,
    (CAST(t.blob_gas_used AS double) * CAST(t.blob_gas_price AS double) / 1e18) * p.price AS fee_usd
FROM blob_transactions_filtered t
JOIN mapping_data q
    ON (q.from_address IS NULL OR t."from" = q.from_address) 
    AND (q.to_address IS NULL OR t."to" = q.to_address) 
    AND (q.method IS NULL OR t.method = q.method)
INNER JOIN prices_filtered p
    ON p."minute" = date_trunc('minute', t.slot_time)
