{{ config(
    schema = 'rollup_economics_ethereum'
    , alias = 'l1_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'day']
    , post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable"]\') }}'
)}}

WITH l1_data AS (
    SELECT
        date_trunc('day', block_time) as day
        , name
        , SUM(data_fee_native) as data_fee_native
        , SUM(data_fee_usd) as data_fee_usd
    FROM {{ ref('rollup_economics_ethereum_l1_data_fees')}}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    GROUP BY 1, 2
)

, l1_verification AS (
    SELECT
        date_trunc('day', block_time) as day
        , name
        , SUM(verification_fee_native) as verification_fee_native
        , SUM(verification_fee_usd) as verification_fee_usd
    FROM {{ ref('rollup_economics_ethereum_l1_verification_fees')}}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    GROUP BY 1, 2
),

l1_blobs AS (
    SELECT
        date_trunc('day', block_time) as day
        , name
        , SUM(blob_fee_native) as blob_fee_native
        , SUM(blob_fee_usd) as blob_fee_usd
    FROM {{ ref('rollup_economics_ethereum_l1_blob_fees')}}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    GROUP BY 1, 2
)

SELECT
    COALESCE(d.day, v.day, b.day) as day
    , COALESCE(d.name, v.name, b.name) as name
    , COALESCE(data_fee_native, 0) AS data_fee_native
    , COALESCE(data_fee_usd, 0) AS data_fee_usd
    , COALESCE(verification_fee_native, 0) AS verification_fee_native
    , COALESCE(verification_fee_usd, 0) AS verification_fee_usd
    , COALESCE(blob_fee_native, 0) AS blob_fee_native
    , COALESCE(blob_fee_usd, 0) AS blob_fee_usd
    , COALESCE(data_fee_native, 0) + COALESCE(verification_fee_native, 0) + COALESCE(blob_fee_native, 0) AS l1_fee_native
    , COALESCE(data_fee_usd, 0) + COALESCE(verification_fee_usd, 0) + COALESCE(blob_fee_usd, 0) AS l1_fee_usd
FROM l1_data d
FULL OUTER JOIN l1_verification v
    ON d.day = v.day 
    AND d.name = v.name
FULL OUTER JOIN l1_blobs b
    ON d.day = b.day 
    AND d.name = b.name