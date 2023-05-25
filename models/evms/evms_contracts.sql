{{ config(
        alias ='contracts',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('ethereum', 'contracts')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('polygon', 'contracts') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('bnb', 'contracts') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('avalanche_c', 'contracts') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('gnosis', 'contracts') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('fantom', 'contracts') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('optimism', 'contracts') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('arbitrum', 'contracts') }}