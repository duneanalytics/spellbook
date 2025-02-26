{{ config(
    schema='gas',
    alias='usage_by_address',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    partition_by=['blockchain'],
    unique_key=['address', 'blockchain', 'currency_symbol'],
    post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "fantom", "polygon", "base", "celo", "zora", "zksync", "scroll", "linea", "zkevm"]\',
                        "sector",
                        "gas",
                        \'["gashawk, zohjag"]\') }}'
) }}

WITH
{% if is_incremental() %}
current_total_metrics AS (
    -- Get the oldest block_time in the current materialized table
    SELECT address, blockchain, currency_symbol, number_of_txs, gas_spent_usd_total, gas_spent_native_total, last_block_time_for_total_metrics
    FROM {{ this }}
),

last_year_window AS (
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        block_time,
        tx_fee as gas_cost_native,
        tx_fee_usd as gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol is not null
    AND block_time >= now() - INTERVAL '1' YEAR
),

current_metrics AS (
    SELECT
        address,
        blockchain,
        currency_symbol,
        COALESCE(ct.number_of_txs, 0) + COUNT(*) FILTER (where block_time > lyw.last_block_time_for_total_metrics) as number_of_txs,
        COALESCE(ct.gas_spent_usd_total, 0) + SUM(CASE WHEN block_time > lyw.last_block_time_for_total_metrics THEN lyw.gas_cost_usd END) as gas_spent_usd_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN lyw.gas_cost_usd END) as gas_spent_usd_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN lyw.gas_cost_usd END) as gas_spent_usd_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN lyw.gas_cost_usd END) as gas_spent_usd_30_days,
        SUM(lyw.gas_cost_usd) as gas_spent_usd_1_year,
        COALESCE(ct.gas_spent_native_total, 0) + SUM(CASE WHEN block_time > lyw.last_block_time_for_total_metrics THEN lyw.gas_cost_native END) as gas_spent_native_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN lyw.gas_cost_native END) as gas_spent_native_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN lyw.gas_cost_native END) as gas_spent_native_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN lyw.gas_cost_native END) as gas_spent_native_30_days,
        SUM(lyw.gas_cost_native) as gas_spent_native_1_year,
        MAX(lyw.block_time) as last_block_time_for_total_metrics
    FROM last_year_window lyw
    FULL OUTER JOIN current_total_metrics ct
    ON ct.address = lyw.address 
    AND ct.blockchain = lyw.blockchain 
    AND ct.currency_symbol = lyw.currency_symbol
    GROUP BY 1, 2, 3
)

{% else %}

current_metrics AS (
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
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_native END) as gas_spent_native_1_year,
        MAX(block_time) as last_block_time_for_total_metrics
    FROM {{ source('gas', 'fees') }}
    GROUP BY 1, 2, 3
)
{% endif %}

SELECT *
FROM current_metrics
ORDER BY gas_spent_usd_total DESC