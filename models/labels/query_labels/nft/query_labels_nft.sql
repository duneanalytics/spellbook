{{config(alias='nft',
    materialized = 'table',
    file_format = 'delta')}}

SELECT * FROM {{ ref('query_labels_nft_traders_transactions') }}
UNION
SELECT * FROM {{ ref('query_labels_nft_traders_volume_usd') }}
UNION
SELECT * FROM {{ ref('query_labels_nft_users_platforms') }}