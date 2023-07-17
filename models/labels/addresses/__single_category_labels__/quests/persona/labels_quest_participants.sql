{{
    config(
        alias = alias('quest_participants'),
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_optimism_coinbase_wallet_quest_participants') }}
UNION ALL
SELECT * FROM {{ ref('labels_optimism_optimism_quest_participants') }}