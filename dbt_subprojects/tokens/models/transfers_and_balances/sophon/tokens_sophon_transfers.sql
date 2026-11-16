{{config(
     schema = 'tokens_sophon',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'table',
     tags = ['static'],
     file_format = 'delta'
     , post_hook='{{ hide_spells() }}'
     )
}}

-- Sophon mainnet launch date: 2024-10-22 (Block #1 timestamp)
{{ transfers_enrich(
    base_transfers = ref('tokens_sophon_base_transfers'),
    transfers_start_date = '2024-10-22',
    blockchain = 'sophon'
  ) 
}}
