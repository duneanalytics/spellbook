{{ config(
        alias ='erc1155_approvalsforall',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('erc1155_ethereum', 'evt_approvalforall')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('erc1155_polygon', 'evt_approvalforall') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('erc1155_bnb', 'evt_approvalforall') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('erc1155_avalanche_c', 'evt_approvalforall') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('erc1155_gnosis', 'evt_approvalforall') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('erc1155_fantom', 'evt_approvalforall') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('erc1155_optimism', 'evt_approvalforall') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('erc1155_arbitrum', 'evt_approvalforall') }}