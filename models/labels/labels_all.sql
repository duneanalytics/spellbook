{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                "sector",
                                "labels",
                                \'["soispoke","hildobby"]\') }}')
}}

-- Static Labels
SELECT * FROM {{ ref('labels_cex') }}
UNION
SELECT * FROM {{ ref('labels_funds') }}
UNION
SELECT * FROM {{ ref('labels_submitted_contracts') }}
UNION
SELECT * FROM {{ ref('labels_ofac_sanctionned_ethereum') }}
UNION
-- Query Labels
SELECT * FROM {{ ref('labels_nft') }}
UNION
SELECT * FROM {{ ref('labels_safe_ethereum') }}
UNION
SELECT * FROM {{ ref('labels_tornado_cash') }}