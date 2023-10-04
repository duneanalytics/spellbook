{{ config(
    tags=['dunesql'],
    alias = alias('tx_hash_labels_all'),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tx_hash_labels",
                                \'["gentrexha"]\') }}')
}}

-- Query Labels
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_stable_to_stable') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_early_investment') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_staking_token_investment') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_harvest_yield') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_onramp') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_offramp') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_bluechip_investment') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_treasury_management') }}
