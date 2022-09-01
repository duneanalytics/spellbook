{{config(alias='nft')}}

SELECT * FROM {{ ref('query_labels_nft_traders_transactions') }}
UNION
SELECT * FROM {{ ref('query_labels_nft_traders_volume_usd') }}
