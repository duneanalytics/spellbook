{{ config(
    alias = 'addresses',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "fantom"]\',
                                "sector",
                                "labels",
                                \'["soispoke","hildobby","ilemi"]\') }}')
}}

-- single category labels (no subsets), needs label_type and model_name added still.
SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type FROM {{ ref('labels_aztec_v2_contracts_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('labels_balancer_v2_pools') }}
UNION ALL
SELECT * FROM {{ ref('labels_cex') }}
UNION ALL
SELECT * FROM {{ ref('labels_contracts') }}
UNION ALL
SELECT * FROM {{ ref('labels_hackers_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('labels_ofac_sanctionned_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('labels_project_wallets') }}
UNION ALL
SELECT * FROM {{ ref('labels_safe_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('labels_tornado_cash') }}
UNION ALL
SELECT * FROM {{ ref('labels_quest_participants') }}
UNION ALL
SELECT * FROM {{ ref('labels_cex_users') }}

-- new/standardized labels
UNION ALL
SELECT * FROM {{ ref('labels_bridges') }}
UNION ALL
SELECT * FROM {{ ref('labels_dex') }}
UNION ALL
SELECT * FROM {{ ref('labels_social') }}
UNION ALL
SELECT * FROM {{ ref('labels_nft') }}
UNION ALL
SELECT * FROM {{ ref('labels_airdrop') }}
UNION ALL
SELECT * FROM {{ ref('labels_dao') }}
UNION ALL
SELECT * FROM {{ ref('labels_infrastructure') }}