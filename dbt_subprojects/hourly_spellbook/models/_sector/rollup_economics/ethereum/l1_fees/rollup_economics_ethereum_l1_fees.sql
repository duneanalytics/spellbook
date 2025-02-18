{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'l1_fees',
    materialized = 'incremental',
    unique_key = ['origin_key', 'tx_hash'],
    incremental_strategy = 'delete+insert',
    on_schema_change = 'sync_all_columns',
    full_refresh = check_mapping_hash('l1')
) }}

WITH mapping_data AS (
    SELECT *
    FROM {{ source("growthepie", "l2economics_mapping", database="dune") }}
    WHERE settlement_layer = 'l1'
    {% if is_incremental() %}
        AND {{ incremental_predicate('created_at') }}
    {% endif %}
),

transactions_filtered AS (
    SELECT 
        t.*,
        bytearray_substring(t."data", 1, 4) AS method
    FROM {{ source('ethereum', 'transactions') }} t
    WHERE t.block_time >= TIMESTAMP '2020-01-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('t.block_time') }}
    {% endif %}
),

prices_filtered AS (
    SELECT p.*
    FROM {{ source('prices', 'usd') }} p
    WHERE p.blockchain IS NULL
        AND p.symbol = 'ETH'
        AND p.minute >= TIMESTAMP '2020-01-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('p.minute') }}
    {% endif %}
)

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
    t.method,
    t.gas_price,
    t.gas_used,
    t.max_fee_per_gas,
    t.max_priority_fee_per_gas,
    t.priority_fee_per_gas,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used,
    LENGTH(t."data") AS data_length,
    CAST(t.gas_used AS double) * CAST(t.gas_price AS double) / 1e18 AS fee_native,
    (CAST(t.gas_used AS double) * CAST(t.gas_price AS double) / 1e18) * p.price AS fee_usd
FROM transactions_filtered t
JOIN mapping_data q
    ON (q.from_address IS NULL OR t."from" = q.from_address) 
    AND (q.to_address IS NULL OR t."to" = q.to_address) 
    AND (q.method IS NULL OR t.method = q.method)
INNER JOIN prices_filtered p
    ON p."minute" = date_trunc('minute', t.block_time)