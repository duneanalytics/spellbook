{{
    config(
        alias = alias('likely_bot_labels'),
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_optimism_likely_bot_addresses') }}
UNION ALL
SELECT * FROM {{ ref('labels_optimism_likely_bot_contracts') }}