{{ config(
    schema='gas',
    alias='usage_by_address',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    partition_by=['blockchain'],
    unique_key=['address', 'blockchain', 'currency_symbol'],
) }}

WITH new_data AS (
    -- Process only new transactions using incremental predicate
    SELECT 
        blockchain,
        tx_from AS address,
        currency_symbol,
        block_time,
        tx_fee AS gas_cost_native,
        tx_fee_usd AS gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol IS NOT NULL
    {% if is_incremental() %}
        AND block_time >= now() - INTERVAL '1' YEAR 
    {% endif %}
),

aggregated_new_data AS (
    -- Aggregate new data
    SELECT
        address,
        blockchain,
        currency_symbol,
        COUNT(*) AS number_of_txs,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN gas_cost_usd END) AS gas_spent_usd_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN gas_cost_usd END) AS gas_spent_usd_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN gas_cost_usd END) AS gas_spent_usd_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_usd END) AS gas_spent_usd_1_year,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN gas_cost_native END) AS gas_spent_native_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN gas_cost_native END) AS gas_spent_native_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN gas_cost_native END) AS gas_spent_native_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_native END) AS gas_spent_native_1_year
    FROM new_data
    GROUP BY 1, 2, 3
),

existing_data AS (
    -- Load existing historical totals (only in incremental runs)
    {% if is_incremental() %}
        SELECT * FROM {{ this }} -- Load existing totals
    {% else %}
        -- Create an empty table structure for full refresh
        SELECT
            NULL AS address,
            NULL AS blockchain,
            NULL AS currency_symbol,
            0 AS number_of_txs,
            0 AS gas_spent_usd_total,
            0 AS gas_spent_usd_24_hours,
            0 AS gas_spent_usd_7_days,
            0 AS gas_spent_usd_30_days,
            0 AS gas_spent_usd_1_year,
            0 AS gas_spent_native_total,
            0 AS gas_spent_native_24_hours,
            0 AS gas_spent_native_7_days,
            0 AS gas_spent_native_30_days,
            0 AS gas_spent_native_1_year
        WHERE 1=0 -- Ensures no data is returned in full-refresh mode
    {% endif %}
),

final_metrics AS (
    -- Merge old totals with new data to ensure accumulation
    SELECT
        COALESCE(e.address, n.address) AS address,
        COALESCE(e.blockchain, n.blockchain) AS blockchain,
        COALESCE(e.currency_symbol, n.currency_symbol) AS currency_symbol,
        
        COALESCE(e.number_of_txs, 0) + COALESCE(n.number_of_txs, 0) AS number_of_txs,
        
        COALESCE(e.gas_spent_usd_total, 0) + COALESCE(n.gas_spent_usd_total, 0) AS gas_spent_usd_total,
        COALESCE(n.gas_spent_usd_24_hours, 0) AS gas_spent_usd_24_hours,
        COALESCE(n.gas_spent_usd_7_days, 0) AS gas_spent_usd_7_days,
        COALESCE(n.gas_spent_usd_30_days, 0) AS gas_spent_usd_30_days,
        COALESCE(n.gas_spent_usd_1_year, 0) AS gas_spent_usd_1_year,

        COALESCE(e.gas_spent_native_total, 0) + COALESCE(n.gas_spent_native_total, 0) AS gas_spent_native_total,
        COALESCE(n.gas_spent_native_24_hours, 0) AS gas_spent_native_24_hours,
        COALESCE(n.gas_spent_native_7_days, 0) AS gas_spent_native_7_days,
        COALESCE(n.gas_spent_native_30_days, 0) AS gas_spent_native_30_days,
        COALESCE(n.gas_spent_native_1_year, 0) AS gas_spent_native_1_year
    FROM aggregated_new_data n
    FULL OUTER JOIN existing_data e
    ON n.address = e.address AND n.blockchain = e.blockchain AND n.currency_symbol = e.currency_symbol
)

SELECT *
FROM final_metrics
ORDER BY gas_spent_usd_total DESC;
