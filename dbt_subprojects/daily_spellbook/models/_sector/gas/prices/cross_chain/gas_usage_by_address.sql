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
{% if is_incremental()  %}
current_metrics AS (
    -- Get the oldest block_time in the current materialized table
    SELECT address, 
        blockchain, 
        currency_symbol, 
        number_of_txs, 
        gas_spent_usd_total, 
        gas_spent_native_total, 
        gas_spent_native_1_year, 
        gas_spent_usd_1_year, 
        last_block_time_of_incremental_update
    FROM {{ this }}
),

last_30d_window AS (
    SELECT 
        blockchain,
        tx_from as address,
        currency_symbol,
        block_time,
        tx_fee as gas_cost_native,
        tx_fee_usd as gas_cost_usd
    FROM {{ source('gas', 'fees') }}
    WHERE currency_symbol is not null
    AND block_time >= now() - INTERVAL '30' DAY
),

window_to_forget AS (
    SELECT
        tx_from as address,
        blockchain,
        currency_symbol,
        SUM(tx_fee_usd) as gas_spent_usd,
        SUM(tx_fee) as gas_spent_native
    FROM {{ source('gas', 'fees') }} gf
    LEFT JOIN current_metrics cm
        ON cm.address = gf.tx_from 
        AND cm.blockchain = gf.blockchain 
        AND cm.currency_symbol = gf.currency_symbol
    WHERE block_time > cm.last_block_time_of_incremental_update - INTERVAL '1' YEAR 
        AND block_time < cm.last_block_time_of_incremental_update - INTERVAL '1' YEAR + INTERVAL '{{var('DBT_ENV_INCREMENTAL_TIME')}}' {{var('DBT_ENV_INCREMENTAL_TIME_UNIT')}}
    GROUP BY 1, 2, 3
),

fresh_metrics AS (
    SELECT
        COALESCE(cm.address, lw.address) AS address,
        COALESCE(cm.blockchain, lw.blockchain) AS blockchain,
        COALESCE(cm.currency_symbol, lw.currency_symbol) AS currency_symbol,
        COUNT(*) FILTER (where block_time > cm.last_block_time_of_incremental_update) as added_number_of_txs,
        SUM(CASE WHEN block_time > cm.last_block_time_of_incremental_update THEN lw.gas_cost_usd END) as added_gas_spent_usd_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN lw.gas_cost_usd END) as fresh_gas_spent_usd_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN lw.gas_cost_usd END) as fresh_gas_spent_usd_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN lw.gas_cost_usd END) as fresh_gas_spent_usd_30_days,
        SUM(CASE WHEN incremental_predicate('block_time') THEN lw.gas_cost_usd END)as added_gas_spent_usd_1_year,
        SUM(CASE WHEN block_time > cm.last_block_time_of_incremental_update THEN lw.gas_cost_native END) as added_gas_spent_native_total,
        SUM(CASE WHEN block_time >= now() - INTERVAL '1' DAY THEN lw.gas_cost_native END) as fresh_gas_spent_native_24_hours,
        SUM(CASE WHEN block_time >= now() - INTERVAL '7' DAY THEN lw.gas_cost_native END) as fresh_gas_spent_native_7_days,
        SUM(CASE WHEN block_time >= now() - INTERVAL '30' DAY THEN lw.gas_cost_native END) as fresh_gas_spent_native_30_days,
        SUM(CASE WHEN incremental_predicate('block_time') THEN lw.gas_cost_native END)as added_gas_spent_native_1_year,
        MAX(lw.block_time) as last_block_time_of_incremental_update
    FROM last_30d_window lw
    FULL OUTER JOIN current_metrics cm
        ON cm.address = lw.address 
        AND cm.blockchain = lw.blockchain 
        AND cm.currency_symbol = lw.currency_symbol
    GROUP BY 1, 2, 3
),

updated_metrics AS (
    SELECT
        COALESCE(cm.address, fm.address) AS address,
        COALESCE(cm.blockchain, fm.blockchain) AS blockchain,
        COALESCE(cm.currency_symbol, fm.currency_symbol) AS currency_symbol,
        COALESCE(cm.number_of_txs, 0) + fm.added_number_of_txs as number_of_txs,
        COALESCE(cm.gas_spent_usd_total, 0) + fm.added_gas_spent_usd_total as gas_spent_usd_total,
        fm.fresh_gas_spent_usd_24_hours as gas_spent_usd_24_hours,
        fm.fresh_gas_spent_usd_7_days as gas_spent_usd_7_days,
        fm.fresh_gas_spent_usd_30_days as gas_spent_usd_30_days,
        COALESCE(cm.gas_spent_usd_1_year, 0) + fm.added_gas_spent_usd_1_year - COALESCE(wtf.gas_spent_usd, 0) as gas_spent_usd_1_year,
        COALESCE(cm.gas_spent_native_total, 0) + fm.added_gas_spent_native_total as gas_spent_native_total,
        fm.fresh_gas_spent_native_24_hours as gas_spent_native_24_hours,
        fm.fresh_gas_spent_native_7_days as gas_spent_native_7_days,
        fm.fresh_gas_spent_native_30_days as gas_spent_native_30_days,
        COALESCE(cm.gas_spent_native_1_year, 0) + fm.added_gas_spent_native_1_year - COALESCE(wtf.gas_spent_native, 0) as gas_spent_native_1_year,
        fm.last_block_time_of_incremental_update as last_block_time_of_incremental_update
    FROM fresh_metrics fm
    FULL OUTER JOIN current_metrics cm
        ON cm.address = fm.address 
        AND cm.blockchain = fm.blockchain 
        AND cm.currency_symbol = fm.currency_symbol
    LEFT JOIN window_to_forget wtf
        ON wtf.address = lw.address 
        AND wtf.blockchain = lw.blockchain 
        AND wtf.currency_symbol = lw.currency_symbol
)

{% else %}

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
),

updated_metrics AS (
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
        MAX(block_time) as last_block_time_of_incremental_update
    FROM gas_costs
    GROUP BY 1, 2, 3
)
{% endif %}

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
    COALESCE(gas_spent_native_1_year, 0) as gas_spent_native_1_year,
    last_block_time_of_incremental_update
FROM updated_metrics
ORDER BY gas_spent_usd_total DESC