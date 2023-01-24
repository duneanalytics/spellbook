{{ config(
        alias ='accounting_interest',
        materialized = 'incremental',
        partition_by = ['code'],
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "maker",
                                \'["lyt", "adcv", "SebVentures", "steakhouse"]\') }}'
        )
}}

WITH interest_accruals_1 AS (
    SELECT i    AS         ilk
         , call_block_time ts
         , call_tx_hash    hash
         , dart
         , NULL AS         rate
    FROM {{ source('maker_ethereum', 'vat_call_frob') }}
    WHERE call_success
      AND dart <> 0.0
      {% if is_incremental() %}
      AND call_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}

    UNION ALL

    SELECT i AS ilk
            , call_block_time ts
            , call_tx_hash hash
            , dart
            , 0.0 AS rate
    FROM {{ source('maker_ethereum', 'vat_call_grab') }}
    WHERE call_success
      AND dart <> 0.0
      {% if is_incremental() %}
      AND call_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}

    UNION ALL

    SELECT i AS ilk
            , call_block_time ts
            , call_tx_hash hash
            , NULL AS dart
            , rate
    FROM {{ source('maker_ethereum', 'vat_call_fold') }}
    WHERE call_success
      AND rate <> 0.0
      {% if is_incremental() %}
      AND call_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
)
, interest_accruals_2 AS (
    SELECT *
         , SUM(dart) OVER (PARTITION BY ilk ORDER BY ts ASC) AS cumulative_dart
    FROM interest_accruals_1
)
, interest_accruals_3 AS (
    SELECT STRING(UNHEX(TRIM('0', RIGHT(ilk, LENGTH(ilk) - 2)))) AS ilk
         , ts
         , hash
         , SUM(cumulative_dart * rate) / POW(10, 45)             AS interest_accruals
    FROM interest_accruals_2
    WHERE rate IS NOT NULL
    GROUP BY 1,2,3
)
, interest_accruals AS (
    SELECT ts
         , hash
         , equity_code            AS code
         , SUM(interest_accruals) AS value --increased equity
         , interest_accruals_3.ilk
    FROM interest_accruals_3
    LEFT JOIN {{ ref('maker_ethereum_helper_ilk_list_labeled') }} ilk_list_labeled
        ON interest_accruals_3.ilk = ilk_list_labeled.ilk
        AND interest_accruals_3.ts BETWEEN COALESCE(ilk_list_labeled.begin_dt, '2000-01-01')
        AND COALESCE(ilk_list_labeled.end_dt, '2222-12-31') --if null, ensure its not restrictive
    GROUP BY 1,2,3,5

    UNION ALL

    SELECT ts
         , hash
         , asset_code             AS code
         , SUM(interest_accruals) AS value --increased assets
         , interest_accruals_3.ilk
    FROM interest_accruals_3
    LEFT JOIN {{ ref('maker_ethereum_helper_ilk_list_labeled') }} ilk_list_labeled
        ON interest_accruals_3.ilk = ilk_list_labeled.ilk
        AND CAST(interest_accruals_3.ts AS DATE) BETWEEN COALESCE(ilk_list_labeled.begin_dt, '2000-01-01')
        AND COALESCE(ilk_list_labeled.end_dt, '2222-12-31') --if null, ensure its not restrictive
    GROUP BY 1,2,3,5
)

SELECT ts,
       hash,
       code,
       value,
       'DAI'               AS token,
       'Interest Accruals' AS descriptor,
       ilk
FROM interest_accruals
;