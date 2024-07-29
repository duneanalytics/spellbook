{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'l1_fees',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['name', 'day'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable"]\') }}'
)}}

WITH l1_data AS (
    SELECT
    date_trunc('day',block_time) as day,
    name,
    SUM(gas_spent) as l1_data_fee,
    SUM(gas_spent_usd) as l1_data_fee_usd
    FROM {{ ref('l1_data_fees')}}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1,2
),

l1_verification AS (
    SELECT
    date_trunc('day',block_time) as day,
    name,
    SUM(gas_spent) as l1_verification_fee,
    SUM(gas_spent_usd) as l1_verification_fee_usd
    FROM {{ ref('l1_verification_fees')}}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1,2
),

l1_blobs AS (
    SELECT
    date_trunc('day',block_time) as day,
    name,
    SUM(blob_spend) as l1_blob_fee,
    SUM(blob_spend_usd) as l1_blob_fee_usd
    FROM {{ ref('l1_blob_fees')}}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1,2
)

SELECT
d.day,
d.name,
l1_data_fee, 
l1_data_fee_usd,
l1_verification_fee,
l1_verification_fee_usd,
l1_blob_fee,
l1_blob_fee_usd,
l1_data_fee + l1_verification_fee + l1_blob_fee AS l1_fee,
l1_data_fee_usd + l1_verification_fee_usd + l1_blob_fee_usd AS l1_fee_usd
FROM l1_data d
LEFT JOIN l1_verification v
    ON v.day = d.day 
    AND v.name = d.name
LEFT JOIN l1_blobs b
    ON b.day = d.day 
    AND b.name = d.name

