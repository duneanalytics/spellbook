{{config(
     schema = 'tokens_sophon',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['block_date','unique_key'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
     post_hook='{{ expose_spells(blockchains = \'["sophon"]\',
                                 spell_type = "sector",
                                 spell_name = "tokens",
                                 contributors = \'["hosuke"]\') }}'
  )
}}

-- Sophon mainnet launch date: 2024-10-22 (Block #1 timestamp)
{{ transfers_enrich(
    base_transfers = ref('tokens_sophon_base_transfers'),
    tokens_erc20_model = source('tokens', 'erc20'),
    prices_model = source('prices', 'hour'),
    transfers_start_date = '2024-10-22',
    blockchain = 'sophon'
  ) 
}}
