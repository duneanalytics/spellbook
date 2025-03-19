{{ config(
    schema='gas',
    alias='usage_by_address',
    materialized='table',
    file_format='delta',
    partition_by = ['address_prefix'],
    unique_key=['address', 'blockchain', 'currency_symbol'],
    options={
        'delta': {
            'Z-ORDER BY': ['address']
        },
    },
    post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zora", "zksync", "scroll", "linea", "zkevm"]\',
                        "sector",
                        "gas",
                        \'["gashawk", "zohjag"]\') }}'
) }}

WITH 
gas_costs AS (
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        block_time,
        tx_fee as gas_cost_native,
        tx_fee_usd as gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE tx_fee > 0 OR tx_fee_usd > 0
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
        SUM(CASE WHEN block_time >= now() - INTERVAL '6' MONTH THEN gas_cost_usd END) as gas_spent_usd_6_months,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_usd END) as gas_spent_usd_1_year,
        SUM(gas_cost_native) as gas_spent_native_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN gas_cost_native END) as gas_spent_native_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN gas_cost_native END) as gas_spent_native_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN gas_cost_native END) as gas_spent_native_30_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '6' MONTH THEN gas_cost_native END) as gas_spent_native_6_months,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_native END) as gas_spent_native_1_year
    FROM gas_costs
    GROUP BY 1, 2, 3
)

SELECT 
    address,
    CAST(SUBSTRING(LOWER(CAST(address AS VARCHAR)), 3, 2) AS VARCHAR) as address_prefix,
    blockchain,
    currency_symbol,
    number_of_txs,
    COALESCE(gas_spent_usd_total, 0) as gas_spent_usd_total,
    COALESCE(gas_spent_usd_24_hours, 0) as gas_spent_usd_24_hours,
    COALESCE(gas_spent_usd_7_days, 0) as gas_spent_usd_7_days,
    COALESCE(gas_spent_usd_30_days, 0) as gas_spent_usd_30_days,
    COALESCE(gas_spent_usd_6_months, 0) as gas_spent_usd_6_months,
    COALESCE(gas_spent_usd_1_year, 0) as gas_spent_usd_1_year,
    COALESCE(gas_spent_native_total, 0) as gas_spent_native_total,
    COALESCE(gas_spent_native_24_hours, 0) as gas_spent_native_24_hours,
    COALESCE(gas_spent_native_7_days, 0) as gas_spent_native_7_days,
    COALESCE(gas_spent_native_30_days, 0) as gas_spent_native_30_days,
    COALESCE(gas_spent_native_6_months, 0) as gas_spent_native_6_months,
    COALESCE(gas_spent_native_1_year, 0) as gas_spent_native_1_year
FROM final_metrics
ORDER BY gas_spent_usd_total DESC
