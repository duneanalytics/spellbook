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

SELECT
    blockchain
    , quester_address as address
    , platform || ': ' || 'participant' AS name
    , 'quests' as category
    , 'msilb7' as contributor
    , 'query' as source
    , MIN(block_time) created_at
    , now() AS updated_at
    , replace(platform,' ', '_') || '_participants' model_name
    , 'persona' as label_type

FROM {{ ref('quests_completions_legacy') }}
GROUP BY 1,2,3,4,5,6,7,8,9,10 -- distinct if addresses completed quests multiple times
