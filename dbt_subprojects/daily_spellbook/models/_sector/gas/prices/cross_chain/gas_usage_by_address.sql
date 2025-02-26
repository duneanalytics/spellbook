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
                        \'["gashawk"]\') }}'
) }}

WITH
{% if is_incremental() %}
min_date AS (
    -- Get the oldest block_time in the current materialized table
    SELECT MIN(block_time) as min_block_time
    FROM {{ source('gas', 'fees') }}
    WHERE block_time IN (
        SELECT MIN(block_time)
        FROM {{ this }}
    )
),

historical_backfill AS (
    -- Only run if the oldest data is newer than 5 years ago
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        COUNT(*) as number_of_txs,
        SUM(tx_fee_usd) as gas_spent_usd_total,
        0 as gas_spent_usd_24_hours, -- These time-bound metrics aren't relevant for historical data
        0 as gas_spent_usd_7_days,
        0 as gas_spent_usd_30_days,
        0 as gas_spent_usd_1_year,
        SUM(tx_fee) as gas_spent_native_total,
        0 as gas_spent_native_24_hours,
        0 as gas_spent_native_7_days,
        0 as gas_spent_native_30_days,
        0 as gas_spent_native_1_year
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol is not null
    AND block_time < (SELECT min_block_time FROM min_date)
    AND block_time >= now() - INTERVAL '5' YEAR
    GROUP BY 1, 2, 3
),
{% endif %}

gas_costs AS (
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
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' YEAR THEN gas_cost_native END) as gas_spent_native_1_year
    FROM gas_costs
    GROUP BY 1, 2, 3
)

{% if is_incremental() %}
, combined_metrics AS (
    -- Combine current metrics with historical backfill
    SELECT
        COALESCE(c.address, h.address) as address,
        COALESCE(c.blockchain, h.blockchain) as blockchain,
        COALESCE(c.currency_symbol, h.currency_symbol) as currency_symbol,
        COALESCE(c.number_of_txs, 0) + COALESCE(h.number_of_txs, 0) as number_of_txs,
        COALESCE(c.gas_spent_usd_total, 0) + COALESCE(h.gas_spent_usd_total, 0) as gas_spent_usd_total,
        COALESCE(c.gas_spent_usd_24_hours, 0) as gas_spent_usd_24_hours,
        COALESCE(c.gas_spent_usd_7_days, 0) as gas_spent_usd_7_days,
        COALESCE(c.gas_spent_usd_30_days, 0) as gas_spent_usd_30_days,
        COALESCE(c.gas_spent_usd_1_year, 0) as gas_spent_usd_1_year,
        COALESCE(c.gas_spent_native_total, 0) + COALESCE(h.gas_spent_native_total, 0) as gas_spent_native_total,
        COALESCE(c.gas_spent_native_24_hours, 0) as gas_spent_native_24_hours,
        COALESCE(c.gas_spent_native_7_days, a0) as gas_spent_native_7_days,
        COALESCE(c.gas_spent_native_30_days, 0) as gas_spent_native_30_days,
        COALESCE(c.gas_spent_native_1_year, 0) as gas_spent_native_1_year
    FROM current_metrics c
    FULL OUTER JOIN historical_backfill h
    ON c.address = h.address 
    AND c.blockchain = h.blockchain 
    AND c.currency_symbol = h.currency_symbol
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
FROM combined_metrics
{% else %}
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
FROM current_metrics
{% endif %}
ORDER BY gas_spent_usd_total DESC