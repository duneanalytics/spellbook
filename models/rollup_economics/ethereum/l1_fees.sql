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
    FROM {{ ref('rollup_economics_l1_data_fees')}}
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
    FROM {{ ref('rollup_economics_l1_verification_fees')}}
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
    FROM {{ ref('rollup_economics_l1_blob_fees')}}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    GROUP BY 1, 2
)

SELECT
    d.day
    , d.name
    , data_fee_native
    , data_fee_usd
    , verification_fee_native
    , verification_fee_usd
    , blob_fee_native
    , blob_fee_usd
    , data_fee_native + verification_fee_native + blob_fee_native AS l1_fee_native
    , data_fee_usd + verification_fee_usd + blob_fee_usd AS l1_fee_usd
FROM l1_data d
FULL OUTER JOIN l1_verification v
    ON d.day = v.day 
    AND d.name = v.name
FULL OUTER JOIN l1_blobs b
    ON d.day = b.day 
    AND d.name = b.name