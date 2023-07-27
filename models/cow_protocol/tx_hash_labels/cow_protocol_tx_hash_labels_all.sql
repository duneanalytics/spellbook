{{ config(
	tags=['legacy'],
	
    alias = alias('tx_hash_labels_all', legacy_model=True),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tx_hash_labels",
                                \'["gentrexha"]\') }}')
}}

-- Query Labels
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_stable_to_stable_legacy') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_early_investment_legacy') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_staking_token_investment_legacy') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_harvest_yield_legacy') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_onramp_legacy') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_offramp_legacy') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_bluechip_investment_legacy') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_treasury_management_legacy') }}
