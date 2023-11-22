{{ config(
        tags = ['static'],
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "optimism", "arbitrum", "polygon", "bitcoin", "fantom"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')
}}


SELECT 'evms' AS blockchain, address, cex_name, distinct_name, added_by, added_date
FROM {{ ref('cex_evms_addresses') }}