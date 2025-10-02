{{config(
     schema = 'tokens_hyperevm',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['block_date','unique_key'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
     post_hook='{{ expose_spells(blockchains = \'["hyperevm"]\',
                                 spell_type = "sector",
                                 spell_name = "tokens",
                                 contributors = \'["krishhh"]\') }}'
  )
}}

-- HypereVM mainnet launch date: 2025-02-18 (Block #1)
{{ transfers_enrich(
    base_transfers = ref('tokens_hyperevm_base_transfers'),
    transfers_start_date = '2025-02-18',
    blockchain = 'hyperevm'
  ) 
}}
