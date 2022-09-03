{{config(alias='all',
    materialized = 'table',
    file_format = 'delta')}}

SELECT * FROM {{ ref('query_labels_nft') }}
UNION
SELECT * FROM {{ ref('query_labels_gnosis_safe_ethereum') }}
UNION
SELECT * FROM {{ ref('query_labels_tornado_cash') }}
UNION
SELECT * FROM {{ ref('query_labels_decoded_contracts') }}