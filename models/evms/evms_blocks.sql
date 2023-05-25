{{ config(
        alias ='blocks',
        unique_key=['blockchain', 'number'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('ethereum', 'blocks')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('polygon', 'blocks') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('bnb', 'blocks') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('avalanche_c', 'blocks') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('gnosis', 'blocks') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('fantom', 'blocks') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('optimism', 'blocks') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('arbitrum', 'blocks') }}