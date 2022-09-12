{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta')
}}

-- Static Labels
SELECT * FROM {{ ref('labels_cex') }}
UNION
SELECT * FROM {{ ref('labels_funds') }}
UNION
SELECT * FROM {{ ref('labels_submitted_contracts') }}
UNION
-- Query Labels
SELECT * FROM {{ ref('labels_nft') }}
UNION
SELECT * FROM {{ ref('labels_safe_ethereum') }}
UNION
SELECT * FROM {{ ref('labels_tornado_cash') }}
