{{config(alias='nft')}}

SELECT * FROM {{ ref('labels_nft_traders_transactions') }}
UNION
SELECT * FROM {{ ref('labels_nft_traders_volume_usd') }}
UNION
SELECT * FROM {{ ref('labels_nft_users_platforms') }}