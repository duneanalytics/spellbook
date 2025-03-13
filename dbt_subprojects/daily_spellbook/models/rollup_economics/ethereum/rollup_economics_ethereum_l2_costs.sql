{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'l2_costs',
    materialized = 'view',
    unique_key = ['name', 'day'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable"]\') }}'
)}}

WITH l1_fees AS (
    SELECT
        date_trunc('day', block_time) AS "day",
        name,
        origin_key,
        SUM(fee_native) as l1_fee_native,
        SUM(fee_usd) as l1_fee_usd,
        SUM(calldata_gas_used) as calldata_gas_used 
    FROM {{ ref('rollup_economics_ethereum_l1_fees')}}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    GROUP BY 1, 2, 3
),

beacon_fees AS (
    SELECT
        date_trunc('day', beacon_slot_time) as "day",
        name,
        origin_key,
        SUM(fee_native) as beacon_fee_native,
        SUM(fee_usd) as beacon_fee_usd,
        SUM(used_blob_byte_count) as used_blob_byte_count
    FROM {{ ref('rollup_economics_ethereum_beacon_fees')}}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('beacon_slot_time')}}
    {% endif %}
    GROUP BY 1, 2, 3
)

SELECT
    COALESCE(l."day", b."day") as "day",
    COALESCE(l.name, b.name) as name,
    COALESCE(l.origin_key, b.origin_key) as origin_key,
    
    -- l1 ethereum execution
    COALESCE(l1_fee_native, 0) AS l1_fee_native,
    COALESCE(l1_fee_usd, 0) AS l1_fee_usd,
    COALESCE(calldata_gas_used, 0) AS calldata_gas_used,
    
    -- beacon chain ethereum
    COALESCE(beacon_fee_native, 0) AS beacon_fee_native,
    COALESCE(beacon_fee_usd, 0) AS beacon_fee_usd,
    COALESCE(used_blob_byte_count, 0) AS used_blob_byte_count,
    
    -- celestia, altDA
    -- ...

    -- avail, altDA
    -- ...

    -- eigenDA, altDA
    -- ...

    -- totals
    COALESCE(l1_fee_native, 0) + COALESCE(beacon_fee_native, 0) AS total_cost_eth,
    COALESCE(l1_fee_usd, 0) + COALESCE(beacon_fee_usd, 0) AS total_cost_usd
FROM l1_fees l
FULL OUTER JOIN beacon_fees b
    ON l."day" = b."day"
    AND l.origin_key = b.origin_key
