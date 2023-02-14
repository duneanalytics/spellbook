{{ config(
    alias = 'bridge',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                "sector",
                                "labels",
                                \'["ilemi"]\') }}')
}}

SELECT * FROM {{ ref('labels_bridges_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('labels_bridges_fantom') }}
