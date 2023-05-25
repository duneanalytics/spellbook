{{ config(
        alias ='traces_decoded',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('ethereum', 'traces_decoded')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('polygon', 'traces_decoded') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('bnb', 'traces_decoded') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('avalanche_c', 'traces_decoded') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('gnosis', 'traces_decoded') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('fantom', 'traces_decoded') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('optimism', 'traces_decoded') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('arbitrum', 'traces_decoded') }}