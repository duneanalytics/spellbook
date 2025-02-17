{{ config(
    schema='gas',
    alias='usage_by_address',
    partition_by=['block_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['address', 'chain'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_day')]
) }}

WITH chain_info AS (
    SELECT 
        blockchain,
        wrapped_native_token_address,
        native_token_symbol as native_currency
    FROM {{ source('evms', 'info') }}
),

transactions AS (
    SELECT 
        t.blockchain as chain,
        t.block_time,
        date_trunc('month', t.block_time) as block_month,
        date_trunc('day', t.block_time) as block_day,
        t."from" as address,
        t.gas_used,
        t.gas_price,
        COALESCE(t.l1_fee, CAST(0 as decimal(38,0))) as l1_fee,
        i.wrapped_native_token_address,
        i.native_currency
    FROM {{ source('evms', 'transactions') }} t
    INNER JOIN chain_info i ON i.blockchain = t.blockchain
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_day') }}
    {% endif %}
),

gas_costs AS (
    SELECT
        t.address,
        t.chain,
        t.native_currency,
        t.block_time,
        t.gas_used,
        t.gas_price,
        t.l1_fee,
        p.price as token_price,
        (CAST(t.gas_used as decimal(38,0)) * t.gas_price) / 1e18 as gas_cost_native,
        CASE WHEN t.l1_fee > 0 THEN t.l1_fee / 1e18 ELSE 0 END as l1_fee_native,
        (CAST(t.gas_used as decimal(38,0)) * t.gas_price) / 1e18 * p.price as base_gas_cost_usd,
        CASE WHEN t.l1_fee > 0 THEN t.l1_fee / 1e18 * p.price ELSE 0 END as l1_fee_usd
    FROM transactions t
    LEFT JOIN {{ source('prices', 'usd') }} p 
        ON p.blockchain = t.chain
        AND p.contract_address = t.wrapped_native_token_address
        AND date_trunc('minute', t.block_time) = p.minute
),

final_metrics AS (
    SELECT
        address,
        chain,
        native_currency,
        COUNT(*) as number_of_txs,
        SUM(base_gas_cost_usd + l1_fee_usd) as gas_spent_usd_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN base_gas_cost_usd + l1_fee_usd END) as gas_spent_usd_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN base_gas_cost_usd + l1_fee_usd END) as gas_spent_usd_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN base_gas_cost_usd + l1_fee_usd END) as gas_spent_usd_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN base_gas_cost_usd + l1_fee_usd END) as gas_spent_usd_1_year,
        SUM(gas_cost_native + l1_fee_native) as gas_spent_native_curr_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN gas_cost_native + l1_fee_native END) as gas_spent_native_curr_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN gas_cost_native + l1_fee_native END) as gas_spent_native_curr_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN gas_cost_native + l1_fee_native END) as gas_spent_native_curr_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_native + l1_fee_native END) as gas_spent_native_curr_1_year
    FROM gas_costs
    GROUP BY 1, 2, 3
)

SELECT 
    address,
    chain,
    native_currency,
    number_of_txs,
    COALESCE(ROUND(gas_spent_usd_total, 2), 0) as gas_spent_usd_total,
    COALESCE(ROUND(gas_spent_usd_24_hours, 2), 0) as gas_spent_usd_24_hours,
    COALESCE(ROUND(gas_spent_usd_7_days, 2), 0) as gas_spent_usd_7_days,
    COALESCE(ROUND(gas_spent_usd_30_days, 2), 0) as gas_spent_usd_30_days,
    COALESCE(ROUND(gas_spent_usd_1_year, 2), 0) as gas_spent_usd_1_year,
    COALESCE(ROUND(gas_spent_native_curr_total, 2), 0) as gas_spent_native_curr_total,
    COALESCE(ROUND(gas_spent_native_curr_24_hours, 2), 0) as gas_spent_native_curr_24_hours,
    COALESCE(ROUND(gas_spent_native_curr_7_days, 2), 0) as gas_spent_native_curr_7_days,
    COALESCE(ROUND(gas_spent_native_curr_30_days, 2), 0) as gas_spent_native_curr_30_days,
    COALESCE(ROUND(gas_spent_native_curr_1_year, 2), 0) as gas_spent_native_curr_1_year
FROM final_metrics
ORDER BY gas_spent_usd_total DESC