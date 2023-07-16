{{
    config(
        alias=alias('quest_participants'),
        tags=['dunesql'],
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
    , created_at
    , now() AS updated_at
    , replace(platform,' ', '_') || '_participants' model_name
    , 'persona' as label_type

FROM {{ ref('quests_participants') }}
GROUP BY 1,2,3