{{ config(
        alias ='erc721_approvalsforall',
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain, * FROM {{source('erc721_ethereum', 'evt_approvalforall')}}
UNION ALL
SELECT 'polygon' AS blockchain, * FROM {{ source('erc721_polygon', 'evt_approvalforall') }}
UNION ALL
SELECT 'bnb' AS blockchain, * FROM {{ source('erc721_bnb', 'evt_approvalforall') }}
UNION ALL
SELECT 'avalanche_c' AS blockchain, * FROM {{ source('erc721_avalanche_c', 'evt_approvalforall') }}
UNION ALL
SELECT 'gnosis' AS blockchain, * FROM {{ source('erc721_gnosis', 'evt_approvalforall') }}
UNION ALL
SELECT 'fantom' AS blockchain, * FROM {{ source('erc721_fantom', 'evt_approvalforall') }}
UNION ALL
SELECT 'optimism' AS blockchain, * FROM {{ source('erc721_optimism', 'evt_approvalforall') }}
UNION ALL
SELECT 'arbitrum' AS blockchain, * FROM {{ source('erc721_arbitrum', 'evt_approvalforall') }}

