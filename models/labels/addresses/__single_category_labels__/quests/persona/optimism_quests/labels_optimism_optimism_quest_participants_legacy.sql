{{
    config(
	tags=['legacy'],
	
        alias = alias('optimism_quest_participants', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

with 
 questers as (
    select quester_address, 'optimism' AS blockchain, COUNT(*) AS num_quests_completed
    from {{ref('optimism_quests_optimism_quest_completions_legacy')}}
    GROUP BY 1,2
  )

select
  blockchain,
  quester_address AS address,
  'Optimism Quests Participant' AS name,
  'quests' AS category,
  'msilb7' AS contributor,
  'query' AS source,
  timestamp('2023-03-11') as created_at,
  now() as updated_at,
  'optimism_quest_participants' as model_name,
  'persona' as label_type
from
  questers

UNION ALL

select
  blockchain,
  quester_address AS address,
  'Optimism Quests - ' || 
  CASE WHEN num_quests_completed >= 10 THEN 'Tier 3'
       WHEN num_quests_completed >= 7 THEN 'Tier 2'
       ELSE 'Tier 1'
  END AS name,
  'quests' AS category,
  'msilb7' AS contributor,
  'query' AS source,
  timestamp('2023-03-11') as created_at,
  now() as updated_at,
  'optimism_quest_participants' as model_name,
  'persona' as label_type
from
  questers
WHERE num_quests_completed >= 4