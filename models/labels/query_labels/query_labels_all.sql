{{config(alias='all')}}

SELECT * FROM {{ ref('query_labels_nft') }}
UNION
SELECT * FROM {{ ref('query_labels_safe_ethereum') }}