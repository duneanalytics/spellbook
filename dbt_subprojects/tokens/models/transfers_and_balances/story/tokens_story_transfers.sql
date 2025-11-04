{{config(
     schema = 'tokens_story',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['block_date','unique_key'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
     post_hook='{{ expose_spells(blockchains = \'["story"]\',
                                 spell_type = "sector",
                                 spell_name = "tokens",
                                 contributors = \'["cursor"]\') }}'
  )
}}

-- Story mainnet launch date: 2024-11-05 (approximate)
{{ transfers_enrich(
    base_transfers = ref('tokens_story_base_transfers'),
    transfers_start_date = '2024-11-05',
    blockchain = 'story'
  ) 
}}
