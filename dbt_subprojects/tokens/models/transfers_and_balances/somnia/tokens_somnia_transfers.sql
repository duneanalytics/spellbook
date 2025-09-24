{{config(
     schema = 'tokens_somnia',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['block_date','unique_key'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
     post_hook='{{ expose_spells(blockchains = \'["somnia"]\',
                                 spell_type = "sector",
                                 spell_name = "tokens",
                                 contributors = \'["krishhh"]\') }}'
  )
}}

-- Somnia mainnet launch date: 2025-05-22 (Block #1)
{{ transfers_enrich(
    base_transfers = ref('tokens_somnia_base_transfers'),
    transfers_start_date = '2025-05-22',
    blockchain = 'somnia'
  ) 
}}
