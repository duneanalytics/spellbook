{{ config(
        alias ='logs',
        unique_key=['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('ethereum', 'logs')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('polygon', 'logs') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('bnb', 'logs') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('avalanche_c', 'logs') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('gnosis', 'logs') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('fantom', 'logs') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('optimism', 'logs') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('arbitrum', 'logs') }}