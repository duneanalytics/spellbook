{{
    config(
        alias=alias('galxe_quest_participants', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\', 
        "sector", 
        "labels", 
        \'["msilb7"]\') }}'
    )
}}

with 
 questers as (
    select token_transfer_to AS address, 'optimism' AS blockchain
    from {{ref('galxe_optimism_nft_mints_legacy')}}
    GROUP BY 1
  )
select
  blockchain,
  address,
  "Galxe Quest Participant" AS name,
  "quests" AS category,
  "msilb7" AS contributor,
  "query" AS source,
  timestamp('2023-06-15') as created_at,
  now() as updated_at,
  "galxe_quest_participants" as model_name,
  "persona" as label_type
from
  questers