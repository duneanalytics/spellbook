{{
    config(
        alias = alias('likely_bot_labels'),
        tags = ['dunesql'],
        post_hook='{{ expose_spells(\'["optimism","base"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

-- op_chains includes optimism, so we don't union the optimism labels here
SELECT * FROM {{ ref('labels_op_chains_likely_bot_addresses') }}
UNION ALL
SELECT * FROM {{ ref('labels_op_chains_likely_bot_contracts') }}