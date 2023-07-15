{{
    config(
	tags=['legacy'],
	
        alias = alias('quest_participants', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type FROM {{ ref('labels_optimism_coinbase_wallet_quest_participants_legacy') }}
UNION ALL
SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type FROM {{ ref('labels_optimism_optimism_quest_participants_legacy') }}
UNION ALL
SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type FROM {{ ref('labels_optimism_galxe_quest_participants_legacy') }}
