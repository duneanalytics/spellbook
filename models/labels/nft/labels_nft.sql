{{config(alias='nft',
        post_hook='{{ expose_spells(\'["ethereum","solana"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}'
)}}

SELECT * FROM {{ ref('labels_nft_traders_transactions') }}
UNION
SELECT * FROM {{ ref('labels_nft_traders_volume_usd') }}
UNION
SELECT * FROM {{ ref('labels_nft_users_platforms') }}