{{config(alias='project_wallets',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["msilb7"]\') }}'
)}}

SELECT
    'optimism' as blockchain,
    address,
    project_name,
    'static' AS source,
    cast('2023-01-28' as date) as created_at,
    NOW() AS updated_at
FROM {{ ref('addresses_optimism_grants_funding') }}
GROUP BY 1,2,3

-- UNION ALL