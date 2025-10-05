{{config(
     schema = 'tokens_peaq',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['block_date','unique_key'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
     post_hook='{{ expose_spells(blockchains = \'["peaq"]\',
                                 spell_type = "sector",
                                 spell_name = "tokens",
                                 contributors = \'["krishhh"]\') }}'
  )
}}

-- Peaq mainnet launch date: 2024-04-25 (Block #1)
{{ transfers_enrich(
    base_transfers = ref('tokens_peaq_base_transfers'),
    transfers_start_date = '2024-04-25',
    blockchain = 'peaq'
  ) 
}}
