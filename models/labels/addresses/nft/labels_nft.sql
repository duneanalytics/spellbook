{{config(alias='nft',
        post_hook='{{ expose_spells(\'["ethereum","solana"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}'
)}}

SELECT * FROM {{ ref('labels_nft_traders_transactions') }}
UNION ALL
SELECT * FROM {{ ref('labels_nft_traders_volume_usd') }}
UNION ALL
SELECT * FROM {{ ref('labels_nft_users_platforms') }}
