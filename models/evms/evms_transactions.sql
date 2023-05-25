{{ config(
        alias ='transactions',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('ethereum', 'transactions')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('polygon', 'transactions') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('bnb', 'transactions') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('avalanche_c', 'transactions') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('gnosis', 'transactions') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('fantom', 'transactions') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('optimism', 'transactions') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('arbitrum', 'transactions') }}