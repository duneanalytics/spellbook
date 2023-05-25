{{ config(
        alias ='logs_decoded',
        unique_key=['blockchain', 'tx_hash'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('ethereum', 'logs_decoded')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('polygon', 'logs_decoded') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('bnb', 'logs_decoded') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('avalanche_c', 'logs_decoded') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('gnosis', 'logs_decoded') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('fantom', 'logs_decoded') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('optimism', 'logs_decoded') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('arbitrum', 'logs_decoded') }}