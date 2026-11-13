{{config(
     schema = 'tokens_story',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'table',
     tags = ['static'],
     file_format = 'delta'
     , post_hook='{{ hide_spells() }}'
     )
}}

{{ transfers_enrich(
    base_transfers = ref('tokens_story_base_transfers'),
    transfers_start_date = '2025-01-23',
    blockchain = 'story'
  ) 
}}
