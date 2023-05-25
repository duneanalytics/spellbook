{{ config(
        alias ='creation_traces',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('ethereum', 'creation_traces')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('polygon', 'creation_traces') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('bnb', 'creation_traces') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('avalanche_c', 'creation_traces') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('gnosis', 'creation_traces') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('fantom', 'creation_traces') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('optimism', 'creation_traces') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('arbitrum', 'creation_traces') }}