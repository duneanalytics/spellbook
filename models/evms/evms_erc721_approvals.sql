{{ config(
        alias ='erc721_approvals',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('erc721_ethereum', 'evt_approval')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('erc721_polygon', 'evt_approval') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('erc721_bnb', 'evt_approval') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('erc721_avalanche_c', 'evt_approval') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('erc721_gnosis', 'evt_approval') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('erc721_fantom', 'evt_approval') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('erc721_optimism', 'evt_approval') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('erc721_arbitrum', 'evt_approval') }}