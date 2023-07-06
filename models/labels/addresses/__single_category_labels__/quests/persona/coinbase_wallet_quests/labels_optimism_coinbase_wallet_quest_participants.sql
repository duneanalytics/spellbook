{{
    config(tags=['dunesql'],
        alias = alias('coinbase_wallet_quest_participants'),
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

with 
 questers as (
    select from_hex(quester_address) as quester_address, 'optimism' AS blockchain
    from { ref('coinbase_wallet_quests_optimism_rewards_transfers_legacy') } -- {{ref('coinbase_wallet_quests_optimism_rewards_transfers')}}
    GROUP BY 1
  )
select
  blockchain,
  quester_address AS address,
  'Coinbase Wallet Quest Participant' AS name,
  'quests' AS category,
  'msilb7' AS contributor,
  'query' AS source,
  DATE '2023-03-11'  as created_at,
  now() as updated_at,
  'coinbase_wallet_quest_participants' as model_name,
  'persona' as label_type
from
  questers