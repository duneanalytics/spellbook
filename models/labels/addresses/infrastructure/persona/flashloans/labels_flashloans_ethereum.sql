{{config(
    alias='flashloans_ethereum',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["niftytable"]\') }}'
    )
}}

WITH basic_info AS (
SELECT DISTINCT target                   AS address,
                'infrastructure'         AS category,
                'persona'                AS label_type,
                'flashloan_users'        AS model_name,
                'ethereum'               AS blockchain,
                'Aave v2 Flashloan User' AS name
FROM
    {{ source('aave_v2_ethereum', 'LendingPool_evt_FlashLoan') }}
WHERE
    amount != 0

UNION ALL
SELECT DISTINCT target                   AS address,
                'infrastructure'         AS category,
                'persona'                AS label_type,
                'flashloan_users'        AS model_name,
                'ethereum'               AS blockchain,
                'Aave v3 Flashloan User' AS name
FROM
    {{ source('aave_v3_ethereum', 'Pool_evt_FlashLoan') }}
WHERE
    amount != 0

UNION ALL
SELECT DISTINCT recipient                    AS address,
                'infrastructure'             AS category,
                'persona'                    AS label_type,
                'flashloan_users'            AS model_name,
                'ethereum'                   AS blockchain,
                'Balancer v2 Flashloan User' AS name
FROM
    {{ source('balancer_v2_ethereum', 'Vault_evt_FlashLoan') }}
WHERE
    amount != 0

UNION ALL
SELECT DISTINCT recipient                   AS address,
                'infrastructure'            AS category,
                'persona'                   AS label_type,
                'flashloan_users'           AS model_name,
                'ethereum'                  AS blockchain,
                'Uniswap v3 Flashloan User' AS name
FROM
    {{ source('uniswap_v3_ethereum', 'Pair_evt_Flash') }}
WHERE
    amount0 != 0
)

SELECT *
     , 'niftytable'       AS contributor
     , 'query'            AS source
     , date('2022-10-08') AS created_at
     , NOW()              AS updated_at
FROM basic_info
WHERE address IS NOT NULL