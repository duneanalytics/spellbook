{{ config(
        alias ='erc1155_transferssingle',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('erc1155_ethereum', 'evt_TransferSingle')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('erc1155_polygon', 'evt_TransferSingle') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('erc1155_bnb', 'evt_TransferSingle') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('erc1155_avalanche_c', 'evt_TransferSingle') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('erc1155_gnosis', 'evt_TransferSingle') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('erc1155_fantom', 'evt_TransferSingle') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('erc1155_optimism', 'evt_TransferSingle') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('erc1155_arbitrum', 'evt_TransferSingle') }}