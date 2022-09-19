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
SELECT blockchain, address, name, category, contributor, source, created_at, updated_at FROM {{ ref('aztec_v2_contract_labels') }}
UNION
-- Query Labels
SELECT * FROM {{ ref('labels_nft') }}
UNION
SELECT * FROM {{ ref('labels_safe_ethereum') }}
UNION
SELECT * FROM {{ ref('labels_tornado_cash') }}
