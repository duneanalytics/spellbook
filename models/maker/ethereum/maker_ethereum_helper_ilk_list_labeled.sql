{{ config(
        alias ='helper_ilk_list_labeled',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['ilk', 'end_dt'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "maker",
                                \'["lyt", "adcv", "SebVentures", "steakhouse"]\') }}'
        )
}}
WITH ilk_list AS
(
    SELECT STRING(UNHEX(TRIM('0', RIGHT(ilk, LENGTH(ilk) - 2)))) AS ilk
    FROM
    (
        SELECT i AS ilk
        FROM {{ source('maker_ethereum', 'vat_call_frob') }}
        {% if is_incremental() %}
        WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        GROUP BY i
        UNION ALL
        SELECT ilk
        FROM {{ source('maker_ethereum', 'spot_call_file') }}
        {% if is_incremental() %}
        WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        GROUP BY ilk
        UNION ALL
        SELECT ilk
        FROM {{ source('maker_ethereum', 'jug_call_file') }}
        {% if is_incremental() %}
        WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        GROUP BY ilk
    )
    GROUP BY ilk
)
, ilk_list_manual_input (ilk, begin_dt, end_dt, asset_code, equity_code, apr) AS
(
    --every RWA needs to be listed here to be counted.
    --PSMs not listed will be assumed non-yield-bearing
    --Any ilk listed in here must have complete history (a row with null as the begin month/yr and a row with null as the end month/year, can be same row)
    values
        ('PSM-GUSD-A',CAST(NULL as date),CAST('2022-10-31' as date),13410,CAST(NULL AS NUMERIC(38)),CAST(NULL AS NUMERIC(38))), --could make rate 0 as well.
        ('PSM-GUSD-A',CAST('2022-11-01' as date),CAST(NULL as date),13411,31180,0.0125),
        ('RWA001-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA002-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA003-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA004-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA005-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA006-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA007-A',CAST(NULL as date),CAST(NULL as date),12320,31172,CAST(NULL AS NUMERIC(38))),
        ('RWA008-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('RWA009-A',CAST(NULL as date),CAST(NULL as date),12310,31170,CAST(NULL AS NUMERIC(38))),
        ('UNIV2DAIUSDC-A',CAST(NULL as date),CAST(NULL as date),11140,31140,CAST(NULL AS NUMERIC(38))), --need to list all UNIV2% LP that are stable LPs, all else assumed volatile
        ('UNIV2DAIUSDT-A',CAST(NULL as date),CAST(NULL as date),11140,31140,CAST(NULL AS NUMERIC(38)))
)
, ilk_list_labeled AS
(
    SELECT *
    FROM ilk_list_manual_input

    UNION ALL

    SELECT ilk_list.ilk
    , NULL AS begin_dt
    , NULL AS end_dt
    , CASE WHEN ilk LIKE 'ETH-%' THEN 11110
        WHEN ilk LIKE 'WBTC-%' OR ilk = 'RENBTC-A' THEN 11120
        WHEN ilk LIKE 'WSTETH-%' OR ilk LIKE 'RETH-%' OR ilk = 'CRVV1ETHSTETH-A' THEN 11130
        WHEN ilk LIKE 'GUNI%' THEN 11140
        WHEN ilk LIKE 'UNIV2%' THEN 11141
        WHEN ilk LIKE 'DIRECT%' THEN 11210
        WHEN ilk LIKE 'RWA%' THEN 12310 --default rwa into off-chain private credit in case an RWA is not manually listed
        WHEN ilk LIKE 'PSM%' THEN 13410 --defaulting PSMS to non-yielding; exceptions should be listed in manual entry table
        WHEN ilk IN ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A','PAXUSD-A') THEN 11510
        ELSE 11199 --other crypto loans category. all other categories are accounted for in the above logic. SAI included here
        END AS asset_code
    , CASE WHEN ilk LIKE 'ETH-%' THEN 31110
        WHEN ilk LIKE 'WBTC-%' OR ilk = 'RENBTC-A'  THEN 31120
        WHEN ilk LIKE 'WSTETH-%' OR ilk LIKE 'RETH-%' OR ilk = 'CRVV1ETHSTETH-A' THEN 31130
        WHEN ilk LIKE 'GUNI%' THEN 31140
        WHEN ilk LIKE 'UNIV2%' THEN 31141
        WHEN ilk LIKE 'DIRECT%' THEN 31160
        WHEN ilk LIKE 'RWA%' THEN 31170 --default rwa into off-chain private credit in case an RWA is not manually listed
        WHEN ilk LIKE 'PSM%' THEN CAST(NULL AS NUMERIC(38)) --defaulting PSMS to non-yielding; exceptions should be listed in manual entry table
        WHEN ilk IN ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A','PAXUSD-A') THEN 31190
        ELSE 31150 --other crypto loans category. all other categories are accounted for in the above logic. SAI included here
        END AS equity_code
    , CAST(NULL AS NUMERIC(38)) AS apr
    FROM ilk_list
    WHERE ilk NOT IN (SELECT ilk FROM ilk_list_manual_input)
    AND ilk <> 'TELEPORT-FW-A' --Need to look into how to handle teleport and potentially update. Ignoring for now.
)
SELECT ilk,
       begin_dt,
       end_dt,
       asset_code,
       equity_code,
       apr
FROM ilk_list_labeled
;