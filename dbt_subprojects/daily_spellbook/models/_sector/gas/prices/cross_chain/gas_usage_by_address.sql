{{ config(
    schema='gas',
    alias='usage_by_address',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    partition_by = ['blockchain'],
    unique_key=['address', 'blockchain', 'currency_symbol'],
) }}

WITH gas_costs AS (
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        block_time,
        tx_fee as gas_cost_native,
        tx_fee_usd as gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol is not null
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% endif %}
),

final_metrics AS (
    SELECT
        address,
        blockchain,
        currency_symbol,
        COUNT(*) as number_of_txs,
        SUM(gas_cost_usd) as gas_spent_usd_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN gas_cost_usd END) as gas_spent_usd_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN gas_cost_usd END) as gas_spent_usd_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN gas_cost_usd END) as gas_spent_usd_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_usd END) as gas_spent_usd_1_year,
        SUM(gas_cost_native) as gas_spent_native_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN gas_cost_native END) as gas_spent_native_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN gas_cost_native END) as gas_spent_native_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN gas_cost_native END) as gas_spent_native_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_native END) as gas_spent_native_1_year
    FROM gas_costs
    GROUP BY 1, 2, 3
)

SELECT 
    address,
    blockchain,
    currency_symbol,
    number_of_txs,
    COALESCE(gas_spent_usd_total, 0) as gas_spent_usd_total,
    COALESCE(gas_spent_usd_24_hours, 0) as gas_spent_usd_24_hours,
    COALESCE(gas_spent_usd_7_days, 0) as gas_spent_usd_7_days,
    COALESCE(gas_spent_usd_30_days, 0) as gas_spent_usd_30_days,
    COALESCE(gas_spent_usd_1_year, 0) as gas_spent_usd_1_year,
    COALESCE(gas_spent_native_total, 0) as gas_spent_native_total,
    COALESCE(gas_spent_native_24_hours, 0) as gas_spent_native_24_hours,
    COALESCE(gas_spent_native_7_days, 0) as gas_spent_native_7_days,
    COALESCE(gas_spent_native_30_days, 0) as gas_spent_native_30_days,
    COALESCE(gas_spent_native_1_year, 0) as gas_spent_native_1_year
FROM final_metrics
ORDER BY gas_spent_usd_total DESC